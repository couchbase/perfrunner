function OnUpdate(doc, meta) {
	var expiry = Math.round((new Date()).getTime() / 1000) + 7200;
	cronTimer(timerCallback, expiry, meta.id);
}
function timerCallback(docid) {
}
