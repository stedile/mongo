BRIDGE_PORT = 29000;
START_PORT = 31100;
SUBST_HOST = "localhost:31105";

r = new ReplSetTest({name : "addshard4", nodes : 3, startPort : START_PORT});
r.startSet();
var config = r.getReplSetConfig();
config.members[1].priority = 0;
config.members[2].arbiterOnly = true;
r.initiate(config);
//Wait for replica set to be fully initialized - could take some time
//to pre-allocate files on slow systems
r.awaitReplication();

//start a bridge to primary, test ReplSetGetStatus (members.name) and isMaster (primary, me, hosts)
db = r.getPrimary().getDB("test");
host = rs.status().members[0].name.split(':')[0];
startMongoProgram("mongobridge", "--port", BRIDGE_PORT, "--dest", host + ":" + START_PORT,
	"--substitute", host + ":" + START_PORT + "=" + SUBST_HOST);

db = new Mongo("localhost:" + BRIDGE_PORT).getDB("test");
count = 0;
count2 = 0;
for (i = 0; i < 3; i++){
    if (rs.status().members[i].name == SUBST_HOST){
	count++;
    }
}
assert.eq (db.isMaster().primary, SUBST_HOST);
assert.eq (db.isMaster().me, SUBST_HOST);


for (i = 0; i < 3; i++){
    if (db.isMaster().hosts[i] == SUBST_HOST){
	count2++;
    }
}

assert.eq(count, 1);
assert.eq(count2, 1);
db = r.getPrimary().getDB("test");
count = 0;
count2 = 0;
for (i = 0; i < 3; i++){
    if (rs.status().members[i].name == SUBST_HOST){
        count++;
    }
}

for (i = 0; i < 1; i++){
    if (db.isMaster().hosts[i] == SUBST_HOST){
        count2++;
    }
}

assert.eq(count, 0);
assert.eq(count2, 0);