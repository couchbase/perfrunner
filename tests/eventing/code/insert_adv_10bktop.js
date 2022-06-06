function OnUpdate(doc, meta) {
    for (let i = 1; i<= 10 ; i++){
     var req = {"id": "meta.id", "keyspace":{"scope_name":"scope-"+i,"collection_name":"collection-1"}};
    couchbase.insert(bucket1, req, "multi-collection");
     }
}
