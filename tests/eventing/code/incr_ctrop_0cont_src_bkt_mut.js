function OnUpdate(doc,meta){
    var result = couchbase.increment(src,meta);
}

function OnDelete(doc){
}