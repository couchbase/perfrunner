function OnUpdate(doc, meta) {
    var fireAt = new Date(fixed_expiry);
    var context = {docId : meta.id};
    createTimer(timerCallback, fireAt, meta.id, context);
}

function timerCallback(context) {
    var query = SELECT * FROM `bucket-1` USE KEYS[context.docId];
    query.execQuery();
    UPSERT INTO `eventing-bucket-1` (KEY, VALUE) VALUES (context.docId, 'Hello World');
}
