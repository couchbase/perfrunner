var config = {
    _id : "rs3",
    members : [
        {_id : 0, host : "node3:27018"},
        {_id : 1, host : "node4:27019"}
    ]
};

rs.initiate(config);

rs.slaveOk();

rs.status();
