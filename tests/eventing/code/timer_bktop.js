function OnUpdate(doc, meta) {
	expiry = fixed_expiry
	expiry = expiry + Math.floor((Math.random() * fuzz_factor) + 1);
	docTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docid) {
	bucket1[docid]=docid;
}
