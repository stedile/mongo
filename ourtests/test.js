/*var replTest = new ReplSetTest({ name: 'testSet', nodes: 3 });
var nodes = replTest.startSet({ oplogSize: "40" });
replTest.initiate();
get master
var master = replTest.getMaster();
var dbs = [master.getDB("foo")];
replTest.bridge();
replTest.partition(0,1);*/


// var shardTest = new ShardingTest(name: "testSet" , shards: 1 /* numShards */, verbose: 0 /* verboseLevel */, mongos: 1 /* numMongos */, {rs: true, numReplicas: 1, chunksize: 0});

var shardTest = new ShardingTest("testSet" , 1 /* numShards */, 0 /* verboseLevel */, 1 /* numMongos */, {rs: true, numReplicas: 1, chunksize: 0});

var replTest = shardTest._rs[0];
replTest.bridge()