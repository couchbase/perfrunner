function OnUpdate(doc, meta) {
    bucket1[meta.id]=doc["alt_email"];
}

function OnDelete(doc) {
}
