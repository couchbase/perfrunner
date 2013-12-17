var config = {
    _id : "rs2",
    members : [
        {_id : 0, host : "node2:27018"},
        {_id : 1, host : "node3:27019"}
    ]
};

rs.initiate(config);

rs.slaveOk();

rs.status();
