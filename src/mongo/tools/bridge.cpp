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


using namespace mongo;
using namespace std;

int port = 0;
int delay = 0;
bool isMongos = false;
string destUri;
void cleanup( int sig );

class Forwarder {
public:
    Forwarder( MessagingPort &mp ) : mp_( mp ) {
    }
    void operator()() const {
        DBClientConnection dest;
        string errmsg;
        while( !dest.connect( destUri, errmsg ) )
            sleepmillis( 500 );
        Message m;
        while( 1 ) {
            try {
                m.reset();
                if ( !mp_.recv( m ) ) {
                    cout << "end connection " << mp_.psock->remoteString() << endl;
                    mp_.shutdown();
                    break;
                }
                sleepmillis( delay );
                int oldId = m.header()->id;
                if ( m.operation() == dbQuery || m.operation() == dbMsg || m.operation() == dbGetMore ) {
                    bool exhaust = false;
                    if ( m.operation() == dbQuery ) {
                        DbMessage d( m );
                        QueryMessage q( d );                  
                        exhaust = q.queryOptions & QueryOption_Exhaust;
                    }
                    Message response;
                    Message new_response;
                    dest.port().call( m, response );
                    if ( response.empty() ) cleanup(0);
                    mongo::QueryResult* r = (mongo::QueryResult*)response.singleData();
                    mongo::BSONObj o( r->data() );
                    cout << "RESULT: " << o << "\n";
                    if (m.operation() == dbQuery){
                        DbMessage d( m );
                        QueryMessage q( d );
                        cout << "OPERATION: " << q.query.toString() << "\n";
                        if (!isMongos && q.query.toString() == "{ ismaster: 1 }"){
                            BSONObjBuilder newQuery;
                            // Copy all elements of the original query object, but change the hosts field                                                         
                            BSONObjIterator it(o);
                            while ( it.more() ) {
                                BSONElement e = it.next();
                                if ( mongoutils::str::equals( e.fieldName(), "hosts" ) ){
                                    BSONArrayBuilder newElement (newQuery.subarrayStart("hosts"));
                                    BSONObjIterator it2 = BSONArray (e.embeddedObject());
                                    while ( it2.more() ){
                                        BSONElement e2 = it2.next();
                                        string s = e2.toString();
                                        int p = s.find("\"");
                                        s = s.substr(p+1, s.length()-p-2);
                                        //string s = "localhost:12345";
                                        newElement.append(s);
                                    }
                                    newElement.done();
                                }
                                else
                                    newQuery.append(e);
                            }
                            BSONObj o2 = newQuery.obj();
                            cout << "OLD OBJ:" << o << "\n";
                            cout << "NEW OBJ:" << o2 << "\n"; 
                            //now we need to copy back to the QueryResult
                            BufBuilder b( 32768 );
                            b.skip( sizeof( QueryResult ) );
                            {
                                b.appendBuf( o2.objdata() , o2.objsize() );
                            }
                            QueryResult *qr = (QueryResult*)b.buf();
                            qr->_resultFlags() = r->_resultFlags();
                            qr->len = b.len();
                            qr->setOperation( opReply );
                            qr->cursorId = r->cursorId;
                            qr->startingFrom = r->startingFrom;
                            qr->nReturned = r->nReturned;
                            b.decouple();
                            //cout << "OLD RESPONSE IS:" << response.toString() << "\n";
                            new_response.setData( qr , true );
                            //cout << "NEW RESPONSE IS:" << new_response.toString() << "\n";                            
                        }
                        else{
                            new_response = response;
                            cout << "RESPONSE IS:" << new_response.toString() << "\n";
                        }
                        //new_response = response;
                        if (!isMongos && q.query.toString() == "{ replSetGetStatus: 1 }"){
                            cout << "REQUESTED REPLSETGETSTATUS\n";
                        }
                    }

                    mp_.reply( m, new_response, oldId );
                    while ( exhaust ) {
                        MsgData *header = new_response.header();
                        QueryResult *qr = (QueryResult *) header;
                        if ( qr->cursorId ) {
                            new_response.reset();
                            dest.port().recv( new_response );
                            mp_.reply( m, new_response ); // m argument is ignored anyway
                        }
                        else {
                            exhaust = false;
                        }
                    }
                }
                else {
                    dest.port().say( m, oldId );
                }
            }
            catch ( ... ) {
                log() << "caught exception in Forwarder, continuing" << endl;
            }
        }
    }
private:
    MessagingPort &mp_;
};

set<MessagingPort*>& ports ( *(new std::set<MessagingPort*>()) );

class MyListener : public Listener {
public:
    MyListener( int port ) : Listener( "bridge" , "", port ) {}
    virtual void acceptedMP(MessagingPort *mp) {
        ports.insert( mp );
        Forwarder f( *mp );
        boost::thread t( f );
    }
};

auto_ptr< MyListener > listener;


void cleanup( int sig ) {
    ListeningSockets::get()->closeAll();
    for ( set<MessagingPort*>::iterator i = ports.begin(); i != ports.end(); i++ )
        (*i)->shutdown();
    ::_exit( 0 );
}
#if !defined(_WIN32)
void myterminate() {
    rawOut( "bridge terminate() called, printing stack:" );
    printStackTrace();
    ::abort();
}

void setupSignals() {
    signal( SIGINT , cleanup );
    signal( SIGTERM , cleanup );
    signal( SIGPIPE , cleanup );
    signal( SIGABRT , cleanup );
    signal( SIGSEGV , cleanup );
    signal( SIGBUS , cleanup );
    signal( SIGFPE , cleanup );
    set_terminate( myterminate );
}
#else
inline void setupSignals() {}
#endif

void helpExit() {
    cout << "usage mongobridge --port <port> --dest <destUri> [ --delay <ms> ]" << endl;
    cout << "    port: port to listen for mongo messages" << endl;
    cout << "    destUri: uri of remote mongod instance" << endl;
    cout << "    ms: transfer delay in milliseconds (default = 0)" << endl;
    ::_exit( -1 );
}

void check( bool b ) {
    if ( !b )
        helpExit();
}

int main( int argc, char **argv ) {
    static StaticObserver staticObserver;
    cout << "THIS IS A TEST\n";
    setupSignals();

    check( argc == 5 || argc == 7 );

    for( int i = 1; i < argc; ++i ) {
        check( i % 2 != 0 );
        if ( strcmp( argv[ i ], "--port" ) == 0 ) {
            port = strtol( argv[ ++i ], 0, 10 );
        }
        else if ( strcmp( argv[ i ], "--dest" ) == 0 ) {
            destUri = argv[ ++i ];
        }
        else if ( strcmp( argv[ i ], "--delay" ) == 0 ) {
            delay = strtol( argv[ ++i ], 0, 10 );
        }
        else if ( strcmp( argv[ i ], "--mongos" ) == 0 ) {
            isMongos = true;
        }
        else {
            check( false );
        }
    }
    check( port != 0 && !destUri.empty() );

    listener.reset( new MyListener( port ) );
    listener->initAndListen();

    return 0;
}
