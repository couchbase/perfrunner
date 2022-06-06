function OnUpdate(doc, meta) {
    var fuzz = Math.floor(Math.random() * fuzz_factor);
    var fireAt = new Date(fixed_expiry + (fuzz * 1000));
    let timerId = meta.id + "::" + Date.now() + "-" + Math.floor(Math.random() * 9999);
    createTimer(timerCallback, fireAt, timerId);
}

function timerCallback() {
}
