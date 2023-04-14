{
  "indexName": "{{ .Values.exploreNrtsearchMode.testIndexName }}",
  "field":
  [
    {
      "name": "doc_id",
      "type": "ATOM",
      "storeDocValues": true},
    {
      "name": "vendor_name",
      "type": "TEXT" ,
      "search": true,
      "store": true,
      "tokenize": true
    },
    {
      "name": "license_no",
      "type": "INT",
      "multiValued": true,
      "storeDocValues": true
    }
  ]
}
