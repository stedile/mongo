// a replica set's passive nodes should be okay to add as part of a shard config

s = new ShardingTest( "addshard4", 1, 0 , 1 , {useHostname : true});
r = new ReplSetTest({name : "addshard4", nodes : 3, startPort : 31100});
r.startSet();
var config = r.getReplSetConfig();
config.members[2].priority = 0;
r.initiate(config);
//Wait for replica set to be fully initialized - could take some time
//to pre-allocate files on slow systems
r.awaitReplication();
print ("PRINTING STATUS\n\n\n\n\n");
db = connect("localhost:31000/admin");
db.printShardingStatus();
db = connect("localhost:31100/test");
while (rs.status().members[0].state != 1 || rs.status().members[1].state != 2 || rs.status().members[2].state != 2);
printjson (rs.status());
db = connect("localhost:31101/test");
printjson (rs.status());
db = connect("localhost:31102/test");
printjson (rs.status());
print ("DONE PRINTING STATUS");

var master = r.getMaster();
var members = config.members.map(function(elem) { return elem.host; });
var shardName = "addshard4/"+members.join(",");
var invalidShardName = "addshard4/foobar";

print("adding shard "+shardName);

// First try adding shard with the correct replica set name but incorrect hostname
// This will make sure that the metadata for this replica set name is cleaned up
// so that the set can be added correctly when it has the proper hostnames.
assert.throws(function() {s.adminCommand({"addshard" : invalidShardName});});



var result = s.adminCommand({"addshard" : shardName});

printjson(result);
assert.eq(result, true);
r.bridge();

print ("PRINTING STATUS\n\n\n\n\n");
db = connect("localhost:31000/admin");
db.printShardingStatus();
db = connect("localhost:31100/test");
while (rs.status().members[0].state != 1 || rs.status().members[1].state != 2 || rs.status().members[2].state != 2);
printjson (rs.status());
db = connect("localhost:31101/test");
printjson (rs.status());
db = connect("localhost:31102/test");
printjson (rs.status());
print ("DONE PRINTING STATUS");
