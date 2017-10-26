function OnUpdate(doc, meta) {
	var docId = meta.id;
	var query = SELECT * FROM `bucket-1` USE KEYS[:docId];
	query.execQuery();
}

function OnDelete(doc) {
}
