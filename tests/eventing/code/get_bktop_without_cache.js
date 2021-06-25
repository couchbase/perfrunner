function OnUpdate(doc, meta) {
    var result = couchbase.get(src,meta,{"cache":false});
}


function OnDelete(doc) {
}
