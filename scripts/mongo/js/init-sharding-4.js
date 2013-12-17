var shards = [
    "rs1/node1:27018,node2:27019",
    "rs2/node2:27018,node3:27019",
    "rs3/node3:27018,node4:27019",
    "rs4/node4:27018,node1:27019",
];

for (var i = 0; i < shards.length; i++) {
    db.runCommand({
        addshard : shards[i]
    });
    }

db.runCommand({
    enablesharding : "UserDatabase"
});

db.runCommand({
    shardcollection : "UserDatabase.UserTable", key : { _id : 1 }
});

db.printShardingStatus();
