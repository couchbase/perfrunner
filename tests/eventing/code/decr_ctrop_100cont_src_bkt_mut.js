function OnUpdate(doc,meta){
    var result = couchbase.decrement(src,{"id":"000000000000"});
}

function OnDelete(doc){
}