r = new ReplSetTest({name : "addshard4", nodes : 3, startPort : 31100});
r.startSet();
var config = r.getReplSetConfig();
config.members[1].priority = 0;
config.members[2].arbiterOnly = true;
r.initiate(config);
//Wait for replica set to be fully initialized - could take some time
//to pre-allocate files on slow systems
r.awaitReplication();

//start a bridge to primary, test ReplSetGetStatus (members.name) and isMaster (primary, me, hosts)
db = connect("localhost:31100/test");
host = rs.status().members[0].name.split(':')[0];
startMongoProgram("mongobridge", "--port", "29000", "--dest", "localhost:31100", "--subst", 
host + ":31100=localhost:31105");
db = connect("localhost:29000/test");
count = 0;
count2 = 0;
for (i = 0; i < 3; i++){
    if (rs.status().members[i].name == "localhost:31105"){
	count++;
    }
}
assert.eq (db.isMaster().primary, "localhost:31105");
assert.eq (db.isMaster().me, "localhost:31105");


for (i = 0; i < 3; i++){
    if (db.isMaster().hosts[i] == "localhost:31105"){
	count2++;
    }
}

assert.eq(count, 1);
assert.eq(count2, 1);
db = connect("localhost:31100/test");
count = 0;
count2 = 0;
for (i = 0; i < 3; i++){
    if (rs.status().members[i].name == "localhost:31105"){
        count++;
    }
}

for (i = 0; i < 1; i++){
    if (db.isMaster().hosts[i] == "localhost:31105"){
        count2++;
    }
}

assert.eq(count, 0);
assert.eq(count2, 0);
