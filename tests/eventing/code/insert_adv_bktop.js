function OnUpdate(doc, meta) {
    couchbase.insert(bucket1, meta, "multi-collection");
}
