START_PORT = 31100;
BRIDGE_PORT_0 = 26000;
BRIDGE_PORT_1 = 26001;
BRIDGE_PORT_2 = 26002;
SET_NAME = "ReplSet";

r = new ReplSetTest({name : SET_NAME, nodes : 3, startPort : START_PORT});
s = new ShardingTest(SET_NAME, 1, 0, 1, {useHostname: true});
r.startSet();
var config = r.getReplSetConfig();
config.members[2].arbiterOnly = true;
r.initiate(config);
r.awaitReplication();

db = r.getPrimary().getDB("test");
host = rs.status().members[0].name.split(':')[0];

SUBST_HOST_0 = host + ":" + BRIDGE_PORT_0;
SUBST_HOST_1 = host + ":" + BRIDGE_PORT_1;
SUBST_HOST_2 = host + ":" + BRIDGE_PORT_2;
PRIMARY_HOST = host + ":" + START_PORT;
SECONDARY_HOST = host + ":" + 31101;
ARBITER_HOST = host + ":" + 31102;
MONGOS = (s.s0 + '').split(':')[1];
MONGOS_HOST = host + ":" + MONGOS;


//startMongodTest(CONFIG, "--configsvr --nojournal");
//startMongoProgram("mongos", "--configdb", host + ":" + CONFIG, "--port", MONGOS);
//startMongoProgram("mongos", "--fork", "--configdb", host + ":" + CONFIG, "--logpath",
//                  "./data/mongos.log", "--port", MONGOS);*/


startMongoProgram("mongobridge", "--port", BRIDGE_PORT_0, "--dest", host + ":" + START_PORT,
                  "--substitute", PRIMARY_HOST + "=" + SUBST_HOST_0 + "," + SECONDARY_HOST +
                  "=" + SUBST_HOST_1 + "," + ARBITER_HOST + "=" + SUBST_HOST_2);
startMongoProgram("mongobridge", "--port", BRIDGE_PORT_1, "--dest",  SECONDARY_HOST, "--substitute",
                  PRIMARY_HOST + "=" + SUBST_HOST_0 + "," + SECONDARY_HOST + "=" + SUBST_HOST_1 +
                  "," + ARBITER_HOST + "=" + SUBST_HOST_2);
startMongoProgram("mongobridge", "--port", BRIDGE_PORT_2, "--dest",  ARBITER_HOST,
                  "--substitute", PRIMARY_HOST + "=" + SUBST_HOST_0 + "," + SECONDARY_HOST +
                  "=" + SUBST_HOST_1 + "," + ARBITER_HOST + "=" + SUBST_HOST_2);

db = new Mongo(MONGOS_HOST).getDB("admin");
var result = db.adminCommand({"addshard" : "ReplSet/" +
                                 SUBST_HOST_0 + "," + SUBST_HOST_1 + "," + SUBST_HOST_2});
assert.eq(result['ok'], 1);
db = new Mongo(SUBST_HOST_0).getDB("test");
while (rs.status().members[0].state != 1 || rs.status().members[1].state != 2 ||
       rs.status().members[2].state != 7){
    sleep(1000);
}
db = new Mongo(MONGOS_HOST).getDB("test");
db.test.insert({'name': 'Paul'});
assert.eq (db.test.find().length(), 1);
r.stopMaster();
stopMongod(BRIDGE_PORT_0, 9);
db = new Mongo(SUBST_HOST_1).getDB("test");
while (rs.status().members[1].state != 1){
    sleep(1000);
}
db = new Mongo(MONGOS_HOST).getDB("test");
assert.eq(db.test.find().length(), 1);

db = new Mongo(MONGOS_HOST).getDB("test");
db.test.insert({'name': 'Pauline'});
assert.eq(db.test.find().length(), 2);

r.stop(1, 9, true);
stopMongod(BRIDGE_PORT_1, 9);

r.start(0, null, true, true);

startMongoProgram("mongobridge", "--port", BRIDGE_PORT_0, "--dest", host + ":" + START_PORT,
                  "--substitute", PRIMARY_HOST + "=" + SUBST_HOST_0 + "," + SECONDARY_HOST +
                  "=" + SUBST_HOST_1 + "," + ARBITER_HOST + "=" + SUBST_HOST_2);
db = new Mongo(SUBST_HOST_2).getDB("test");
while (rs.status().members[0].state != 1){
    sleep(1000);
}

r.start(1, null, true, true);
startMongoProgram("mongobridge", "--port", BRIDGE_PORT_1, "--dest",  SECONDARY_HOST, "--substitute",
                  PRIMARY_HOST + "=" + SUBST_HOST_0 + "," + SECONDARY_HOST + "=" + SUBST_HOST_1 +
                  "," + ARBITER_HOST + "=" + SUBST_HOST_2);

db = new Mongo(MONGOS_HOST).getDB("test"); 
assert.eq(db.test.find().length(), 1); 
print ("END TEST");