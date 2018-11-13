function OnUpdate(doc, meta) {
    doc["from_onupdate"]=1;
    src[meta.id]=doc;
}

function OnDelete(doc) {
}
