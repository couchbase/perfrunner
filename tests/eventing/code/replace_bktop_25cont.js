function OnUpdate(doc,meta){
    var get_res, replace_res;
    if(meta.id % 20 == 0){
        get_res = couchbase.get(dest,{ "id":"000000000000"});
        replace_res = couchbase.replace(dest, get_res.meta, doc);
        while(replace_res.success == false && replace_res.error.cas_mismatch){
            get_res = couchbase.get(dest, {"id":"000000000000"});
            replace_res = couchbase.replace(dest, get_res.meta, doc);
        }
    }
    else{
        get_res = couchbase.get(dest,meta);
        replace_res=couchbase.replace(dest, get_res.meta, doc);
        while(replace_res.success == false && replace_res.error.cas_mismatch){
               get_res = couchbase.get(dest, meta);
               replace_res = couchbase.replace(dest, get_res.meta, doc);
        }
    }
}


function OnDelete(doc) {
}
