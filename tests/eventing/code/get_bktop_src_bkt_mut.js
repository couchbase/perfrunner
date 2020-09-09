function OnUpdate(doc, meta) {
    var result = couchbase.get(src,meta);
}


function OnDelete(doc) {
}
