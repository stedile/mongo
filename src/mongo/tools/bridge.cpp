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
                    Message newResponse;
                    dest.port().call(m, response);
                    if (response.empty())
                        cleanup(0);
                    mongo::QueryResult* qrpResponse = (mongo::QueryResult*)response.singleData();
                    if (m.operation() == dbQuery) {
                        if (subst) {
                            DbMessage d(m);
                            QueryMessage qmResponse(d);
                            string s = qmResponse.query.firstElement().fieldName();
                            if (s == "isMaster")
                                replaceIsMaster(qrpResponse, &newResponse);
                            
                            else if (s == "replSetGetStatus")
                                replaceReplSetGetStatus(qrpResponse, &newResponse);
                        }
                        
                        else
                            newResponse = response;
                    }
                    
                    mp_.reply(m, newResponse, oldId);
                    while (exhaust) {
                        MsgData *header = newResponse.header();
                        QueryResult *qr = (QueryResult *) header;
                        if (qr->cursorId) {
                            newResponse.reset();
                            dest.port().recv(newResponse);
                            mp_.reply(m, newResponse); // m argument is ignored anyway
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
    
    void replaceIsMaster(QueryResult* qr, Message* newResponse) const {
        mongo::BSONObj data(qr->data());
        BSONObjBuilder newQuery;
        // Copy all elements of the original query object, changing some fields
        BSONObjIterator fieldIterator(data);
        while (fieldIterator.more()) {
            BSONElement field = fieldIterator.next();
            
            if (mongoutils::str::equals(field.fieldName(), "hosts")) {
                BSONArrayBuilder newHost (newQuery.subarrayStart("hosts"));
                BSONObjIterator hostNameIterator (field.embeddedObject());
                while (hostNameIterator.more()) {
                    BSONElement hostName = hostNameIterator.next();
                    string host = hostName.str();
                    if (alias.find(host) != alias.end()) {
                        host = alias[host];
                    }
                    newHost.append(host);
                }
                newHost.done();
            }
            
            else if (mongoutils::str::equals(field.fieldName(), "arbiters")) {
                BSONArrayBuilder newArbiter (newQuery.subarrayStart("arbiters"));
                BSONObjIterator arbiterIterator (field.embeddedObject());
                while (arbiterIterator.more()) {
                    BSONElement arbiter = arbiterIterator.next();
                    string arb = arbiter.str();
                    if (alias.find(arb) != alias.end())
                        arb = alias[arb];
                    newArbiter.append(arb);
                }
                newArbiter.done();
            }
            
            else if (mongoutils::str::equals(field.fieldName(), "passives")) {
                BSONArrayBuilder newPassive (newQuery.subarrayStart("passives"));
                BSONObjIterator passiveIterator (field.embeddedObject());
                while (passiveIterator.more()) {
                    BSONElement passive = passiveIterator.next();
                    string psv = passive.str();
                    if (alias.find(psv) != alias.end())
                        psv = alias[psv];
                    newPassive.append(psv);
                }
                newPassive.done();
            }
            
            else if (mongoutils::str::equals(field.fieldName(), "me")) {
                string me = field.str();
                if (alias.find(me) != alias.end())
                    me = alias[me];
                newQuery.append(field.fieldName(), me);
            }
            
            else if (mongoutils::str::equals(field.fieldName(), "primary")) {
                string primary = field.str();
                if (alias.find(primary) != alias.end())
                    primary = alias[primary];
                newQuery.append(field.fieldName(), primary);
            }
            
            else
                newQuery.append(field);
        }
        BSONObj newData = newQuery.obj();
        //now we need to copy back to the QueryResult
        BufBuilder b(32768);
        b.skip(sizeof(QueryResult));
        {
            b.appendBuf(newData.objdata() , newData.objsize());
        }
        QueryResult *newQr = (QueryResult*)b.buf();
        newQr->_resultFlags() = qr->_resultFlags();
        newQr->len = b.len();
        newQr->setOperation(opReply);
        newQr->cursorId = qr->cursorId;
        newQr->startingFrom = qr->startingFrom;
        newQr->nReturned = qr->nReturned;
        b.decouple();
        newResponse->setData(newQr , true);
    }
    
    void replaceReplSetGetStatus(QueryResult* qr, Message* newResponse) const {
        mongo::BSONObj data(qr->data());
        BSONObjBuilder newQuery;
        BSONObjIterator fieldIterator(data);
        while (fieldIterator.more()) {
            BSONElement field = fieldIterator.next();
            
            if (mongoutils::str::equals(field.fieldName(), "members")) {
                
                BSONArrayBuilder newMemberArray (newQuery.subarrayStart("members"));
                BSONObjIterator memberIterator (field.embeddedObject());
                
                while (memberIterator.more()) {
                    BSONElement member = memberIterator.next();
                    
                    BSONObjBuilder newMemberField;
                    BSONObjIterator memberFieldIterator = BSONObj (member.embeddedObject());
                    
                    while (memberFieldIterator.more()) {
                        BSONElement memberField = memberFieldIterator.next();
                        if (str::equals(memberField.fieldName(), "name")) {
                            string s = memberField.str();
                            if (alias.find(s) != alias.end())
                                s = alias[s];
                            newMemberField.append(memberField.fieldName(), s);
                        }
                        else
                            newMemberField.append(memberField);
                    }
                    
                    newMemberArray.append(newMemberField.obj());
                    
                }
                newMemberArray.done();
            }
            else
                newQuery.append(field);
        }
        
        BSONObj newData = newQuery.obj();
        //now we need to copy back to the QueryResult
        BufBuilder b(32768);
        b.skip(sizeof(QueryResult));
        {
            b.appendBuf(newData.objdata() , newData.objsize());
        }
        QueryResult *newQr = (QueryResult*)b.buf();
        newQr->_resultFlags() = qr->_resultFlags();
        newQr->len = b.len();
        newQr->setOperation(opReply);
        newQr->cursorId = qr->cursorId;
        newQr->startingFrom = qr->startingFrom;
        newQr->nReturned = qr->nReturned;
        b.decouple();
        newResponse->setData(newQr , true);
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
    string aliasList;
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
            aliasList = argv[++i];
        }
        else {
            check(false);
        }
    }
    //Parse the substitution argument
    std::vector<std::string> list;
    
    if (subst) {
        splitStringDelim(aliasList, &list, ',');
        for (size_t i = 0; i < list.size(); i++) {
            std::vector<std::string> list2;
            splitStringDelim(list[i], &list2, '=');
            if (list2.size() != 2) {
                cerr << list[i];
                cerr << " is not a valid format for subst argument.";
                cerr << "Use --help for more info on correct format\n";
                return -1;
            }
            alias[list2[0]] = list2[1];
        }
    }
    
    check(port != 0 && !destUri.empty());
    
    
    listener.reset(new MyListener(port));
    listener->initAndListen();
    
    return 0;
}