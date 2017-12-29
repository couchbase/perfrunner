function OnUpdate(doc, meta) {
	expiry = Math.round((new Date()).getTime() / 1000) + 2400;
	cronTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docid) {
	bucket1[docid]=docid;
}
