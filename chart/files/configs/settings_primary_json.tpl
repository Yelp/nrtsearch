{
  "indexName": "{{ .Values.exploreNrtsearchMode.testIndexName }}",
  "directory": "MMapDirectory",
  "nrtCachingDirectoryMaxSizeMB": 1.0,
  "indexMergeSchedulerAutoThrottle": false,
  "concurrentMergeSchedulerMaxMergeCount": 16,
  "concurrentMergeSchedulerMaxThreadCount": 8
}
