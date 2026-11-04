# Vector benchmark tools

Tools to test vector search on a local nrtsearch cluster, and to compare a replica that leaves
full precision vectors in S3 (lazy remote data) with one that keeps every file on disk.

- `cluster.sh` runs a primary and two isolated replicas as local processes.
- `vector_bench` (installed by `./gradlew installDist`) creates, loads, streams to and queries a
  test index.
- `run_benchmark.sh` runs the standard comparison and saves the results.

## Setup

You need a real S3 bucket: nrtsearch talks to AWS S3 and has no option for another endpoint.

```bash
./gradlew installDist
export BENCH_BUCKET=my-bucket          # required
export AWS_PROFILE=dev                 # or any other way of giving AWS credentials, see below
scripts/vector_bench/cluster.sh start  # primary on 50051, lazy replica on 50061, local on 50071
```

### AWS credentials

The servers use the AWS SDK's default credential chain, from the environment `cluster.sh` is run
in: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, then the `AWS_PROFILE` profile (or `default`)
in `~/.aws/credentials` and `~/.aws/config`, then an instance or container role. Profiles that
assume a role, or that use `credential_process`, work.

SSO profiles (`sso_session`, `sso_start_url`) don't: the SSO modules of the SDK aren't on the
server's classpath. Export temporary keys instead, and restart the nodes when they expire:

```bash
aws sso login --profile my-sso-profile
eval "$(aws configure export-credentials --profile my-sso-profile --format env)"
```

The `botoCfgPath` server setting can't select a profile; it always uses `[default]`.

If a node can't reach the bucket, nrtsearch keeps running with a dummy S3 client and fails later.
`cluster.sh start` checks each node's log for this and stops with the AWS error, including for a
node that was already running. Before starting a replica it also checks that the primary has
written the cluster state to S3 (with the `aws` CLI if installed, otherwise from the primary's
log), since replicas can only read it.

The cluster keeps local data, logs and rendered configs in `~/vector_bench` (`BENCH_HOME`), and
index data in `s3://$BENCH_BUCKET/vector-bench/` (`BENCH_SERVICE`). Run `cluster.sh` without
arguments for all commands and settings.

| Node | Port | What it is |
|---|---|---|
| primary | 50051 | Takes writes. Uploads every refreshed version to S3 (`s3RefreshUpload`). |
| lazy | 50061 | Isolated replica. `.vec` files stay in S3 and are read by range into a block cache. |
| local | 50071 | Isolated replica that downloads every file. The baseline. |

Replicas poll S3 every `POLL_SECONDS` (5) and so lag the primary by a few seconds.

## Standard run

```bash
scripts/vector_bench/run_benchmark.sh
```

This creates a 100,000 vector, 768 dimension index, computes exact neighbors for 1,000 queries,
waits for both replicas, then queries the local replica, the lazy replica with an empty cache, and
the lazy replica warm. The summary looks like:

```
query_lazy_warm:
  Throughput: 412.3 qps over 60.0s
  Latency:    n=24738 err=0 mean=19.30ms p50=17.10ms p90=28.40ms p99=61.20ms ...
  Recall@10:  0.9870 ...
  S3 cache:   hit rate 96.2% (...), S3 41.2 MB (0.7 MB/s), 31 GETs/s, avg block load 21.4ms, ...
Local disk, lazy replica (~/vector_bench/replica_lazy/index):
  .lazy           0.1 MB
  .veq          290.4 MB
  .vex           18.2 MB
  ...
  total         412.0 MB
```

The numbers above are only an illustration of the format. Common variations:

```bash
COUNT=1000000 DURATION=120 scripts/vector_bench/run_benchmark.sh     # bigger index
QPS=200 CONCURRENCY=32 scripts/vector_bench/run_benchmark.sh         # fixed rate, open loop
SKIP_LOAD=1 TOPK=100 scripts/vector_bench/run_benchmark.sh           # same index, other queries
CACHE_TYPE=OFF_HEAP CACHE_SIZE_MB=4096 scripts/vector_bench/cluster.sh restart lazy
```

Change cache settings by restarting the lazy replica with new values, then `SKIP_LOAD=1`.

## vector_bench commands

Every command has `--help`. Commands talk gRPC to one nrtsearch node (`--port`, default 50051),
running the gateway code in process, or to a running vector gateway (`--gateway URL`). The bucket
and index (`--bucket bench --index vectors` by default) are recorded in `--registryFile`
(`vector_bench_registry.json`), which every command must share.

| Command | Does |
|---|---|
| `setup` | Create the bucket and index, deleting an existing index of that name. Run against the primary. |
| `load` | Bulk load vectors with PutVectors. `--threads`, `--batch`, `--start`/`--num` to resume or split. |
| `stream` | Steady write stream: `--rate` vectors/s, `--insertPct`, `--deletePct`, rest are updates. |
| `groundtruth` | Exact neighbors of query vectors by brute force, saved for recall checks. |
| `query` | Query load at `--qps` or `--concurrency`, reporting latency, recall and S3 cache reads. |
| `info` | A node's vector count, searcher version, segments, disk size and cache stats. `--watch 5`. |

### Data

Generated data (default) is a mixture of Gaussian clusters, set by `--count`, `--dim`,
`--clusters`, `--spread` and `--seed`. Vector *i* depends only on the seed and *i*, so loads,
streams and ground truth all agree without storing the data. Clustered data matters: random
vectors have no neighbor structure, which makes HNSW recall and cache locality unrealistic.

Standard datasets in `.fvecs` format work too, for example SIFT1M from
http://corpus-texmex.irisa.fr/:

```bash
FVECS=sift/sift_base.fvecs QUERY_FVECS=sift/sift_query.fvecs COUNT=1000000 \
  scripts/vector_bench/run_benchmark.sh
```

Use `--metric euclidean` for SIFT; it is not normalized.

Every vector gets metadata: `category` (`c0` to `c9`, one tenth each), `bucket` (0 to 99) and a
non-filterable `text`. `groundtruth --filteredPct 20` makes a fifth of the queries filter on one
category, and the exact neighbors respect the filter.

### Streaming writes while querying

```bash
vector_bench stream --rate 500 --duration 600 --insertPct 50 --deletePct 10 &
vector_bench query --port 50061 --queries ~/vector_bench/queries_100000.bin --duration 600
```

Recall is measured against the query set's exact neighbors, which were computed for the data at
the time. Once a stream has inserted, updated or deleted vectors, recall is approximate. To check
recall after a stream, recompute ground truth with the `--count` the stream prints. Deletes and
updates are not reflected in it.

### Reading the query report

- **Latency** is measured from each query's scheduled start. At a fixed `--qps`, if every sender is
  busy, queries queue and the wait is included, so overload shows up as latency rather than being
  hidden. The report warns when the target rate was missed.
- **Recall@K** is the fraction of the exact top K found.
- **S3 cache** is the change in the node's block cache counters over the run: hit rate, data and
  GET requests to S3, and the average time to load a block. It reads the node's metrics, so with
  `--gateway` pass `--metricsPort` for the replica behind it.

## Recreating the index

`setup` deletes and recreates the index on the primary. Running replicas only start indexes
created after they started when told to, so run `cluster.sh reload` after `setup` (the benchmark
script does). `cluster.sh clean` stops everything and deletes local data; data in S3 is kept.
