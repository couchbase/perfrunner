function OnUpdate(doc, meta) {
	var expiry = fixed_expiry;
	expiry = expiry + Math.floor((Math.random() * fuzz_factor) + 1);
	docTimer(timerCallback, meta.id, expiry);
}

function timerCallback(docid) {
}
