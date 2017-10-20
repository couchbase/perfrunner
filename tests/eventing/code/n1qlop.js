function OnUpdate(doc, meta) {
	try {
		var docId = meta.id;
		var query = SELECT * FROM `bucket-1` USE KEYS[:docId] limit 1;
		var list = query.execQuery();
	}
	catch(e) {
		log(e)
	}
}

function OnDelete(doc) {
}
