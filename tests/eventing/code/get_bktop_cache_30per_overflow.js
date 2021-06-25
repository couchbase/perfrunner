function OnUpdate(doc, meta) {
    var key=('000000000000'+(meta.id % 85000)).slice(-12);
    var result = couchbase.get(src,{ "id":key},{"cache": true});
}


function OnDelete(doc) {
}
