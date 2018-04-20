function OnUpdate(doc, meta) {
	docTimer(timerCallback, fixed_expiry, meta.id);
}
function timerCallback(docid) {
	bucket1[docid]=docid;
}
