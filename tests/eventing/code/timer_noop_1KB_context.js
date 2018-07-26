function OnUpdate(doc, meta) {
    var fireAt = new Date(fixed_expiry);
    var context = {docID : meta.id, document: doc};
    createTimer(timerCallback, fireAt, meta.id, context);
}

function timerCallback(context) {
}
