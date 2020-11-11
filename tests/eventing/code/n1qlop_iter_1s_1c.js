function OnUpdate(doc, meta) {
	var query = SELECT * FROM `bucket-1`.`scope-1`.`collection-1` where alt_email >= "000015" and alt_email <= "000028";
	for (var item of query){
	}
}

function OnDelete(doc) {
}
