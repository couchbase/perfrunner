function OnUpdate(doc, meta) {
	expiry = Math.round((new Date()).getTime() / 1000) + 300;
	docTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docid) {
	var query = SELECT * FROM `bucket-1` USE KEYS[:docid];
	query.execQuery();
	query = UPSERT INTO `eventing-bucket-1` ( KEY, VALUE ) VALUES ( :docId, 'Hello World');
	query.execQuery();
}
