nodeName: "lucene_server_primary"
hostName: "primary-node"
port: "{{ .Values.primaryMainPort }}"
replicationPort: "{{ .Values.primaryReplicationPort }}"
stateDir: "{{ .Values.primary.stateDir }}"
indexDir: "{{ .Values.primary.indexDir }}"
threadPoolConfiguration:
  maxSearchingThreads: 4
  maxIndexingThreads: 18
fileSendDelay: false
bucketName: "nrtsearch-bucket"
archiveDirectory: "{{ .Values.primary.archiveDir }}"
serviceName: "nrtsearch-service-test"
restoreState: False
restoreFromIncArchiver: "true"
backupWithIncArchiver: "true"
downloadAsStream: "true"
{{ if ( not .Values.exploreNrtsearchMode.enabled) }}
botoCfgPath: "/user/app/boto.cfg"
{{ end }}
