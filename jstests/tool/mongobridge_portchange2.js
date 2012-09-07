BRIDGE_PORT = 29001;
START_PORT = 31100;
SECONDARY_PORT = 31101;
ARBITER = 31102;
SUBST_HOST_0 = "localhost:31105";
SUBST_HOST_1 = "localhost:31106";
SUBST_HOST_2 = "localhost:31107";



r = new ReplSetTest({name : "addshard4", nodes : 3, startPort : START_PORT});
r.startSet();
var config = r.getReplSetConfig();
config.members[1].priority = 0;
config.members[2].arbiterOnly = true;
r.initiate(config);
//Wait for replica set to be fully initialized - could take some time
//to pre-allocate files on slow systems
r.awaitReplication();
db = r.getPrimary().getDB("test");
host = rs.status().members[0].name.split(':')[0];
str = host + ":" + START_PORT + "=" + SUBST_HOST_0 + ",";
str += host + ":" + SECONDARY_PORT + "=" + SUBST_HOST_1 + ",";
str += host + ":" + ARBITER + "=" + SUBST_HOST_2;

//start a bridge to secondary, test ReplSetGetStatus(memebers.name)
// and isMaster (primary, me, hosts, passive, arbiter)
startMongoProgram("mongobridge", "--port", BRIDGE_PORT, "--dest", host + ":" + SECONDARY_PORT,
		  "--substitute", str);
db = new Mongo("localhost:" + BRIDGE_PORT).getDB("test");
count = 0;
count2 = 0;
count3 = 0;
for (i = 0; i < 3; i++){
    if (rs.status().members[i].name == SUBST_HOST_0)
        count++;
    if (rs.status().members[i].name == SUBST_HOST_1)
      count2++;
    if (rs.status().members[i].name == SUBST_HOST_2)
      count3++;
}
assert.eq(count, 1);
assert.eq(count2, 1);
assert.eq(count3, 1);

assert.eq (db.isMaster().primary, SUBST_HOST_0);
count = 0;
count2 = 0;
count3 = 0;

for (i = 0; i < 1; i++){
    if (db.isMaster().hosts[i] == SUBST_HOST_0)
        count++;
    if (db.isMaster().passives[i] == SUBST_HOST_1)
	count2++;
    if (db.isMaster().arbiters[i] == SUBST_HOST_2)
	count3++;
}

assert.eq(count, 1);
assert.eq(count2, 1);
assert.eq(count3, 1);