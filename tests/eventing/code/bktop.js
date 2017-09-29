function OnUpdate(doc, meta) {
    bucket1[meta.docid]=doc["alt_email"]
}

function OnDelete(doc) {
}
