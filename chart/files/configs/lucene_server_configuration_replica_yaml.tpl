nodeName: "node-name-replica"
hostName: replica-hostname
port: "{{ .Values.replicaMainPort }}"
replicationPort: "{{ .Values.replicaReplicationPort }}"
stateDir: "{{ .Values.replica.stateDir }}"
indexDir: "{{ .Values.replica.indexDir }}"
threadPoolConfiguration:
  maxSearchingThreads: 16
  maxIndexingThreads: 4
bucketName: "nrtsearch-bucket"
archiveDirectory: "{{ .Values.replica.archiveDir }}"
serviceName: "nrtsearch-service-test"
restoreState: False
restoreFromIncArchiver: "true"
backupWithIncArchiver: "true"
downloadAsStream: "true"
{{ if ( not .Values.exploreNrtsearchMode.enabled) }}
botoCfgPath: "/user/app/boto.cfg"
{{ end }}
