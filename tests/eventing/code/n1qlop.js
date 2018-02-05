function OnUpdate(doc, meta) {
	var docId = meta.id;
	var query = SELECT * FROM `bucket-1` USE KEYS[$docId];
	query.execQuery();
	query = UPSERT INTO `eventing-bucket-1` ( KEY, VALUE ) VALUES ( $docId, 'Hello World');
	query.execQuery();
}

function OnDelete(doc) {
}
