function OnUpdate(doc, meta) {
    bucket1[meta.id]=doc["Field_1"];
}

function OnDelete(doc) {
}
