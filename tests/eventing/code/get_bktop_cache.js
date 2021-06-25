function OnUpdate(doc, meta) {
    var result = couchbase.get(src,{ "id":"000000000000"},{"cache": true});
}


function OnDelete(doc) {
}
