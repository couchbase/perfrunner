function OnUpdate(doc, meta) {
	docTimer(timerCallback, fixed_expiry, meta.id);
}

function timerCallback(docId) {
	var query = SELECT * FROM `bucket-1` USE KEYS[$docId];
	query.execQuery();
	UPSERT INTO `eventing-bucket-1` (KEY, VALUE) VALUES ($docId, 'Hello World');
}
