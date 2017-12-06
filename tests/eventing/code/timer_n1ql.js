function OnUpdate(doc, meta) {
	expiry = Math.round((new Date()).getTime() / 1000) + 1800;
	docTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docId) {
	var query = SELECT * FROM `bucket-1` USE KEYS[:docId];
	query.execQuery();
	query = UPSERT INTO `eventing-bucket-1` ( KEY, VALUE ) VALUES ( :docId, 'Hello World');
	query.execQuery();
}
