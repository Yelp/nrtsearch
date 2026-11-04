Multiple Primaries
==========================

A cluster (all nodes sharing a ``serviceName``) can run several primary capable nodes, with a single active
primary at any time. The other primaries are standbys that keep their local index data close to the latest
version in remote storage, so that one of them can take over quickly when the active primary fails.

Enabling
--------

.. code-block:: yaml

  serviceName: my_cluster
  stateConfig:
    backendType: REMOTE
    remote:
      readOnly: false
  indexStartConfig:
    autoStart: true
    mode: PRIMARY
    dataLocationType: REMOTE
  s3RefreshUpload: true
  s3RefreshNotifyAfterUpload: true
  leaderElection:
    type: s3_lease
    params:
      ttlMs: 15000

Startup fails if any of the settings above are not compatible with leader election.

Roles
-----

* **Standby**: started indices follow index data from remote storage, polling like an isolated replica. All
  state changing requests (indexing, commit, create/delete index, field and settings updates) and primary
  replication requests are rejected with ``FAILED_PRECONDITION``; the error message names the current leader.
* **Active**: indices are started as primary, with ``primaryGen`` equal to the leadership epoch.

When a standby is elected, it claims all remote state, then restarts its indices as primary. Only index files
that differ from the local copy are downloaded. When the active primary loses leadership, it stops writing
remote state immediately and restarts its indices as standby followers.

State refresh
-------------

The active primary is the source of global state (index creation, start, deletion, field and settings
changes). Standbys follow it by refreshing from remote storage periodically
(``leaderElection.refreshIntervalMs``, default 30s) and on ``ReloadState`` requests. With leader election,
``ReloadState`` requests a refresh and waits up to ``leaderElection.reloadWaitMs`` (default 60s) for one that
started after the request; if it does not finish in time the request fails with ``DEADLINE_EXCEEDED`` and the
refresh continues in the background. On the active primary, ``ReloadState`` does nothing. A refresh never runs
during a role change, and stops early if one is pending; it is run again afterwards. A refresh checks for a
pending role change between indices, so in the worst case a promotion waits for one standby index to finish
restoring from remote storage. A refresh also stops standby indices that are no longer started in global state,
and a newly elected primary stops any such index before starting the others as primary.

Durability
----------

A newly elected primary starts from the latest index version in remote storage, which is the last commit (or
the last refresh upload). Documents that were added but not committed on the old primary are lost and must be
re-sent by the client. A commit is only acknowledged once its data is durable in remote storage under the
current leader, so acknowledged commits are never lost on failover.

Because ``s3RefreshNotifyAfterUpload`` is required, replicas only advance to index versions that are durable in
remote storage, so they are never ahead of the version a new primary starts from.

Fencing
-------

Leader election alone cannot guarantee that the old primary has stopped, for example during a long GC pause or
a network partition. Correctness does not rely on it. Every remote ``_current`` pointer (global state, index
state, point state, warming queries) records the epoch of the primary that wrote it, and is updated with a
compare-and-set:

* On promotion, the new primary claims each pointer by rewriting it with its epoch, failing if a newer epoch
  already owns it.
* After a pointer is claimed, every write to it by an older primary fails, no matter how long that primary was
  paused. The old primary then resigns and becomes a standby.
* A write that the old primary completed before the claim is seen by the new primary, which restores from it.
* A pointer that does not exist yet is claimed by creating an empty (tombstone) pointer owned by the new epoch, so
  an old primary can not create it afterwards. Servers and tools treat a tombstone as a missing resource.

Since S3 derives an object's ETag from its content, every fenced pointer write also changes the pointer content:
a fenced ``_current`` object contains the resource name, then a newline and a ``#nrt-fence epoch=<E> nonce=<N>``
marker. Servers and ``nrt_utils`` tools read the resource name as everything before the ``\n#nrt-fence `` marker (including the trailing space),
or the whole content if there is no marker (unfenced pointers). A marker without a valid ``epoch`` is reported as
a malformed pointer, and promotion fails rather than claiming state it cannot order; fix the pointer with
``nrt_utils`` before retrying. Servers from before multi-primary support cannot read fenced pointers, so all nodes
of a cluster must be upgraded before enabling leader election.

Replicas reject replication requests with a lower ``primaryGen`` than they have already seen
(``enforcePrimaryGenOrdering``, enabled automatically with leader election).

Custom electors
---------------

Plugins can provide an elector by implementing ``PrimaryElectorPlugin`` and returning a
``PrimaryElectorProvider`` for a type name, which can then be used in ``leaderElection.type``. An elector must
provide strictly increasing epochs across leadership changes, must report ``isLeader() == false`` as soon as
leadership may have been lost, must make ``resign(epoch)`` give up leadership only if it is still held at that
epoch (checked atomically), and must not acquire leadership after ``close()``. A faulty elector can delay failover, but cannot corrupt index data, since writes
are fenced as described above.
