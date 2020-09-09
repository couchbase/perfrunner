function OnUpdate(doc,meta){
    var result = couchbase.increment(dest,meta);
}

function OnDelete(doc){
}