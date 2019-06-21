function OnUpdate(doc, meta) {
	var docId = meta.id;
	var select_query = SELECT * FROM `bucket-1` USE KEYS[$docId];
	for (var r of select_query) {
	}
	select_query.close()
	var upsert_query = UPSERT INTO `eventing-bucket-1` (KEY, VALUE) VALUES ($docId, 'Hello World');
	upsert_query.close();
}

function OnDelete(meta) {
}
