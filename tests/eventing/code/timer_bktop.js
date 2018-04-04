function OnUpdate(doc, meta) {
	docTimer(timerCallback, meta.id, fixed_expiry);
}
function timerCallback(docid) {
	bucket1[docid]=docid;
}
