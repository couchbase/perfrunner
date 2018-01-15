function OnUpdate(doc, meta) {
	expiry = fixed_expiry
	expiry = expiry + Math.floor((Math.random() * fuzz_factor) + 1);
	cronTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docid) {
	var query = SELECT * FROM `bucket-1` USE KEYS[:docId];
	query.execQuery();
	query = UPSERT INTO `eventing-bucket-1` ( KEY, VALUE ) VALUES ( :docId, 'Hello World');
	query.execQuery();
}
