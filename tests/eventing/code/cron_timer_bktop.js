function OnUpdate(doc, meta) {
	var expiry = fixed_expiry;
	expiry = expiry + Math.floor((Math.random() * fuzz_factor) + 1);
	cronTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docid) {
	bucket1[docid]=docid;
}
