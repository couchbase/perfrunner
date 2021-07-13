function OnUpdate(doc, meta) {
    for(var i =0;i<8;i++){
        var result = couchbase.get(src,{ "id":"000000000000"},{"cache": false});
    }
}


function OnDelete(doc) {
}
