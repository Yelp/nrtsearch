{
  "indexName": "{{ .Values.exploreNrtsearchMode.testIndexName }}",
  "startHit": 0,
  "topHits": 100,
  "retrieveFields": ["doc_id", "license_no", "vendor_name"],
  "queryText": "vendor_name:first vendor"
}
