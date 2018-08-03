function OnUpdate(doc, meta) {
    var fuzz = Math.floor(Math.random() * fuzz_factor);
    var fireAt = new Date(fixed_expiry + (fuzz * 1000));
    createTimer(timerCallback, fireAt, meta.id);
}

function timerCallback() {
}
