// bridge.cpp
/**
 *    Copyright (C) 2008 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "pch.h"

#include <string>

#include <boost/thread.hpp>

#include "mongo/db/dbmessage.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/net/message.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/stringutils.h"

using namespace mongo;
using namespace std;

int port = 0;
int delay = 0;
bool subst = false;
string destUri;
void cleanup(int sig);
std::map<std::string, string> alias; //stores map of host alias

class Forwarder {
public:
    Forwarder(MessagingPort &mp) : mp_(mp) {
    }
    
    void operator()() const {
        DBClientConnection dest;
        string errmsg;
        while(!dest.connect(destUri, errmsg))
            sleepmillis(500);
        Message m;
        while(1) {
            try {
                m.reset();
                if (!mp_.recv(m)) {
                    cout << "end connection " << mp_.psock->remoteString() << endl;
                    mp_.shutdown();
                    break;
                }
                sleepmillis(delay);
                
                int oldId = m.header()->id;
                if (m.operation() == dbQuery || m.operation() == dbMsg || m.operation() == dbGetMore) {
                    bool exhaust = false;
                    if (m.operation() == dbQuery) {
                        DbMessage d(m);
                        QueryMessage q(d);
                        exhaust = q.queryOptions & QueryOption_Exhaust;
                    }
                    Message response;
                    Message new_response;
                    dest.port().call(m, response);
                    if (response.empty()) 
                        cleanup(0);
                    mongo::QueryResult* r = (mongo::QueryResult*)response.singleData();
                    mongo::BSONObj o(r->data());
                    if (m.operation() == dbQuery){
                        if (subst){
                            DbMessage d(m);
                            QueryMessage q(d);
                            string s = q.query.firstElement().fieldName();
                
                            if (s == "isMaster")
                                replace_isMaster(r, &new_response);
                            
                            else if (s == "replSetGetStatus")
                                replace_replSetGetStatus(r, &new_response);
                        }
                        
                        else
                            new_response = response;
                    }

                    mp_.reply(m, new_response, oldId);
                    while (exhaust) {
                        MsgData *header = new_response.header();
                        QueryResult *qr = (QueryResult *) header;
                        if (qr->cursorId) {
                            new_response.reset();
                            dest.port().recv(new_response);
                            mp_.reply(m, new_response); // m argument is ignored anyway
                        }
                        else {
                            exhaust = false;
                        }
                    }
                }
                else {
                    dest.port().say(m, oldId);
                }
            }
            catch (...) {
                log() << "caught exception in Forwarder, continuing" << endl;
            }
        }
    }
    
    void replace_isMaster(QueryResult* r, Message* new_response) const {
        mongo::BSONObj o(r->data());
        BSONObjBuilder newQuery;
        // Copy all elements of the original query object, changing some fields
        BSONObjIterator it(o);
        while (it.more()) {
            BSONElement e = it.next();
            
            if (mongoutils::str::equals(e.fieldName(), "hosts")){
                BSONArrayBuilder newElement (newQuery.subarrayStart("hosts"));
                BSONObjIterator it2(e.embeddedObject());
                //iterates thru hosts
                while (it2.more()){
                    BSONElement e2 = it2.next();
                    //parse text
                    string s = e2.str();
                    if (alias.find(s) != alias.end()){ //matches dic
                        s = alias[s];
                    }
                    newElement.append(s);
                }
                newElement.done();
            }
            
            else if (mongoutils::str::equals(e.fieldName(), "arbiters")){
                BSONArrayBuilder newElement (newQuery.subarrayStart("arbiters"));
                BSONObjIterator it2 = BSONArray (e.embeddedObject());
                while (it2.more()){
                    BSONElement e2 = it2.next();
                    string s = e2.str();
                    if (alias.find(s) != alias.end())
                        s = alias[s];
                    newElement.append(s);
                }
                newElement.done();
            }
            
            else if (mongoutils::str::equals(e.fieldName(), "passives")){
                BSONArrayBuilder newElement (newQuery.subarrayStart("passives"));
                BSONObjIterator it2 = BSONArray (e.embeddedObject());
                while (it2.more()){
                    BSONElement e2 = it2.next();
                    string s = e2.str();
                    if (alias.find(s) != alias.end())
                        s = alias[s];
                    newElement.append(s);
                }
                newElement.done();
            }
            
            else if (mongoutils::str::equals(e.fieldName(), "me")){
                string s = e.str();
                if (alias.find(s) != alias.end())
                    s = alias[s];
                newQuery.append(e.fieldName(), s);
            }
            
            else if (mongoutils::str::equals(e.fieldName(), "primary")){
                string s = e.str();
                if (alias.find(s) != alias.end())
                    s = alias[s];
                newQuery.append(e.fieldName(), s);
            }
            
            else
                newQuery.append(e);
        }
        BSONObj o2 = newQuery.obj();
        //now we need to copy back to the QueryResult
        BufBuilder b(32768);
        b.skip(sizeof(QueryResult));
        {
            b.appendBuf(o2.objdata() , o2.objsize());
        }
        QueryResult *qr = (QueryResult*)b.buf();
        qr->_resultFlags() = r->_resultFlags();
        qr->len = b.len();
        qr->setOperation(opReply);
        qr->cursorId = r->cursorId;
        qr->startingFrom = r->startingFrom;
        qr->nReturned = r->nReturned;
        b.decouple();
        new_response->setData(qr , true);
    }
    
    void replace_replSetGetStatus(QueryResult* r, Message* new_response) const {
        mongo::BSONObj o(r->data());
        BSONObjBuilder newQuery;
        BSONObjIterator it(o);
        while (it.more()) {
            BSONElement e = it.next();
            
            if (mongoutils::str::equals(e.fieldName(), "members")){
                
                BSONArrayBuilder newElement (newQuery.subarrayStart("members"));
                BSONObjIterator it2 = BSONArray (e.embeddedObject());
                //this iterates through the members
                
                while (it2.more()){
                    BSONElement e2 = it2.next();
                    //this is a member, a bson object
                    
                    BSONObjBuilder newElement2; // will hold the new member
                    BSONObjIterator it3 = BSONObj (e2.embeddedObject());
                    
                    while (it3.more()){
                        BSONElement e3 = it3.next(); //field of a member
                        if (str::equals(e3.fieldName(), "name")){
                            string s = e3.str();
                            if (alias.find(s) != alias.end())
                                s = alias[s];
                            newElement2.append(e3.fieldName(), s);
                        }
                        else
                            newElement2.append(e3);
                    }
                    
                    newElement.append(newElement2.obj());
                    
                }
                newElement.done();
            }
            else
                newQuery.append(e);
        }
        
        BSONObj o2 = newQuery.obj();
        //now we need to copy back to the QueryResult
        BufBuilder b(32768);
        b.skip(sizeof(QueryResult));
        {
            b.appendBuf(o2.objdata() , o2.objsize());
        }
        QueryResult *qr = (QueryResult*)b.buf();
        qr->_resultFlags() = r->_resultFlags();
        qr->len = b.len();
        qr->setOperation(opReply);
        qr->cursorId = r->cursorId;
        qr->startingFrom = r->startingFrom;
        qr->nReturned = r->nReturned;
        b.decouple();
        new_response->setData(qr , true);
    }

private:
    MessagingPort &mp_;
};
    


set<MessagingPort*>& ports (*(new std::set<MessagingPort*>()));

class MyListener : public Listener {
public:
    MyListener(int port) : Listener("bridge" , "", port) {}
    virtual void acceptedMP(MessagingPort *mp) {
        ports.insert(mp);
        Forwarder f(*mp);
        boost::thread t(f);
    }
};

auto_ptr< MyListener > listener;


void cleanup(int sig) {
    ListeningSockets::get()->closeAll();
    for (set<MessagingPort*>::iterator i = ports.begin(); i != ports.end(); i++)
        (*i)->shutdown();
    ::_exit(0);
}
#if !defined(_WIN32)
void myterminate() {
    rawOut("bridge terminate() called, printing stack:");
    printStackTrace();
    ::abort();
}

void setupSignals() {
    signal(SIGINT , cleanup);
    signal(SIGTERM , cleanup);
    signal(SIGPIPE , cleanup);
    signal(SIGABRT , cleanup);
    signal(SIGSEGV , cleanup);
    signal(SIGBUS , cleanup);
    signal(SIGFPE , cleanup);
    set_terminate(myterminate);
}
#else
inline void setupSignals() {}
#endif

void helpExit() {
    cout << "usage mongobridge --port <port> --dest <destUri> [--delay <ms> --subst <list>]"
        << endl;
    cout << "    port: port to listen for mongo messages" << endl;
    cout << "    destUri: uri of remote mongod instance" << endl;
    cout << "    ms: transfer delay in milliseconds (default = 0)" << endl;
    cout << "    list: list of uri substitutions; hosts in a pair are separated by = and";
    cout << " pairs are comma separated. Substitutes occurences of the first host in a pair";
    cout << " with the second one in replies to replSetGetStatus and isMaster through the bridge.";
    cout << endl;
    cout << "          (e.g. host1=host2,host3=host4 subtitutes";
    cout << " host1 with host2, and host3 with host 4)" << endl;
    ::_exit(-1);
}

void check(bool b) {
    if (!b)
        helpExit();
}
    
int main(int argc, char **argv) {
    static StaticObserver staticObserver;
    setupSignals();
    check(argc == 5 || argc == 7 || argc == 9);
    string alias_list;
    for(int i = 1; i < argc; ++i) {
        check(i % 2 != 0);
        if (strcmp(argv[i], "--port") == 0) {
            port = strtol(argv[++i], 0, 10);
        }
        else if (strcmp(argv[i], "--dest") == 0) {
            destUri = argv[++i];
        }
        else if (strcmp(argv[i], "--delay") == 0) {
            delay = strtol(argv[++i], 0, 10);
        }
        else if (strcmp(argv[i], "--subst") == 0) {
            subst = true;
            alias_list = argv[++i];
        }
        else {
            check(false);
        }
    }
    //Parse the mongos argument
    std::vector<std::string> list;

    if (subst) {
        splitStringDelim(alias_list, &list, ',');
        for (size_t i = 0; i < list.size(); i++){
            std::vector<std::string> list2;
            splitStringDelim(list[i], &list2, '=');
            if (list2.size() != 2){
                cout << list2.size();
                cout << "Format for subst argument is off. Use --help for more info on correct format\n";
                return 0;
            }
            alias[list2[0]] = list2[1];
        }
    }

    check(port != 0 && !destUri.empty());


    listener.reset(new MyListener(port));
    listener->initAndListen();

    return 0;
}
