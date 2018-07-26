function OnUpdate(doc, meta) {
    var fireAt = new Date(fixed_expiry);
    createTimer(timerCallback, fireAt, meta.id);
}

function timerCallback() {
}
