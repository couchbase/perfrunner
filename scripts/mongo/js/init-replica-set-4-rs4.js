var config = {
    _id : "rs4",
    members : [
        {_id : 0, host : "node4:27018"},
        {_id : 1, host : "node1:27019"}
    ]
};

rs.initiate(config);

rs.slaveOk();

rs.status();
