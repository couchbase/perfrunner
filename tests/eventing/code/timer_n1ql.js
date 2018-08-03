function OnUpdate(doc, meta) {
    var fuzz = Math.floor(Math.random() * fuzz_factor);
    var fireAt = new Date(fixed_expiry + (fuzz * 1000));
    var context = {docId : meta.id};
    createTimer(timerCallback, fireAt, meta.id, context);
}

function timerCallback(context) {
    var query = SELECT * FROM `bucket-1` USE KEYS[context.docId];
    query.execQuery();
    var docID = context.docId;
    UPSERT INTO `eventing-bucket-1` (KEY, VALUE) VALUES ($docID, 'Hello World');
}
