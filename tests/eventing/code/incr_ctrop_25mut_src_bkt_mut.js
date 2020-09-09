function OnUpdate(doc,meta){
    if(meta.id % 25 == 0){
        var result = couchbase.increment(src,{"id":"000000000000"});
    }
    else{
        var result = couchbase.increment(src,meta);
    }
}


function OnDelete(doc) {
}
