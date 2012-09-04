r = new ReplSetTest({name : "addshard4", nodes : 3, startPort : 31100});
r.startSet();
var config = r.getReplSetConfig();
config.members[1].priority = 0;
config.members[2].arbiterOnly = true;
r.initiate(config);
//Wait for replica set to be fully initialized - could take some time
//to pre-allocate files on slow systems
r.awaitReplication();
db = connect("localhost:31100/test");
host = rs.status().members[0].name.split(':')[0];
str = host + ":31100=localhost:31105,";
str += host + ":31101=localhost:31106,";
str += host + ":31102=localhost:31107";
//start a bridge to secondary, test ReplSetGetStatus(memebers.name)
// and isMaster (primary, me, hosts, passive, arbiter)
startMongoProgram("mongobridge", "--port", "29001", "--dest", "localhost:31101", 
"--subst", str);

db = connect("localhost:29001/test");
count = 0;
count2 = 0;
count3 = 0;
for (i = 0; i < 3; i++){
    if (rs.status().members[i].name == "localhost:31105")
        count++;
    if (rs.status().members[i].name == "localhost:31106")
	count2++;
    if (rs.status().members[i].name == "localhost:31107")
	count3++;
}

assert.eq(count, 1);
assert.eq(count2, 1);
assert.eq(count3, 1);

assert.eq (db.isMaster().primary, "localhost:31105");
count = 0;
count2 = 0;
count3 = 0;

for (i = 0; i < 1; i++){
    if (db.isMaster().hosts[i] == "localhost:31105")
        count++;
    if (db.isMaster().passives[i] == "localhost:31106")
	count2++;
    if (db.isMaster().arbiters[i] == "localhost:31107")
	count3++;
}

assert.eq(count, 1);
assert.eq(count2, 1);
assert.eq(count3, 1);