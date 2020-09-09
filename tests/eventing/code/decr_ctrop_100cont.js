function OnUpdate(doc,meta){
    var result = couchbase.decrement(dest,{"id":"000000000000"});
}

function OnDelete(doc){
}