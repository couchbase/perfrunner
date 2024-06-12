function OnUpdate(doc, meta) {
    var temp = new Date().getTime();
    doc['time']=temp;
    bucket1[meta.id]=doc
}

function OnDelete(doc) {
}
