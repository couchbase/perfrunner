function OnUpdate(doc, meta) {
	cronTimer(timerCallback, meta.id, fixed_expiry);
}
function timerCallback(docid) {
	bucket1[docid]=docid;
}
