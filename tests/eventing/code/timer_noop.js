function OnUpdate(doc, meta) {
	expiry = Math.round((new Date()).getTime() / 1000) + 800;
	docTimer(timerCallback, meta.id, expiry);
}

function timerCallback(docid) {
}
