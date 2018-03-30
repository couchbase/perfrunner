function OnUpdate(doc, meta) {
	var expiry = fixed_expiry;
	expiry = expiry + Math.floor((Math.random() * fuzz_factor) + 1);
	docTimer(timerCallback, meta.id, expiry);
}

function timerCallback(docId) {
	var query = SELECT * FROM `bucket-1` USE KEYS[$docId];
	query.execQuery();
	UPSERT INTO `eventing-bucket-1` (KEY, VALUE) VALUES ($docId, 'Hello World');
}
