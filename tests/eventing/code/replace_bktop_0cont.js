function OnUpdate(doc,meta){
    var get_res, replace_res;
    get_res = couchbase.get(dest, meta);
    replace_res = couchbase.replace(dest, get_res.meta, doc);
    while(replace_res.success == false && replace_res.error.cas_mismatch){
            get_res = couchbase.get(dest, meta);
            replace_res = couchbase.replace(dest, get_res.meta, doc);
    }
}


function OnDelete(doc) {
}
