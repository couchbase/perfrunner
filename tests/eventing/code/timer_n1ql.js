function OnUpdate(doc, meta) {
	docTimer(timerCallback, meta.id, fixed_expiry);
}

function timerCallback(docId) {
	var query = SELECT * FROM `bucket-1` USE KEYS[$docId];
	query.execQuery();
	UPSERT INTO `eventing-bucket-1` (KEY, VALUE) VALUES ($docId, 'Hello World');
}
