function OnUpdate(doc,meta){
    var get_res, replace_res;
    get_res = couchbase.get(src, meta);
    replace_res = couchbase.replace(src, get_res.meta, doc);
    while(replace_res.success == false && replace_res.error.cas_mismatch){
            get_res = couchbase.get(src, meta);
            replace_res = couchbase.replace(src, get_res.meta, doc);
    }
}


function OnDelete(doc) {
}
