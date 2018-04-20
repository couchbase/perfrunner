function OnUpdate(doc, meta) {
	cronTimer(timerCallback, fixed_expiry, meta.id);
}
function timerCallback(docid) {
	bucket1[docid]=docid;
}
