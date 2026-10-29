/*
 * Copyright 2026 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.BoolValue;
import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.nrt.pagecache.CompoundFileRanges;
import com.yelp.nrtsearch.server.nrt.pagecache.PageCachePolicyApplier;
import com.yelp.nrtsearch.server.utils.PageCacheUtils;
import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Replicate an index with a quantized vector field, with page cache eviction rules set. */
public class PageCacheEvictReplicationTest {
  private static final String INDEX = "test_index";
  private static final String FIELD = "quantized_vector";
  private static final int DIMENSIONS = 64;
  private static final int NUM_DOCS = 2000;
  private static final String EVICT_CONFIG =
      String.join(
          "\n",
          "mmapReadAdvice: BY_CONTEXT",
          "pageCache:",
          "  rules:",
          "    - files: [\"*QuantizedVectorsFormat_*.vec\"]",
          "      action: EVICT");

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer primaryServer;
  private TestServer replicaServer;

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  @Test
  public void testEvict_noCompoundFile() throws IOException {
    verifyEvict(false);
  }

  @Test
  public void testEvict_compoundFile() throws IOException {
    verifyEvict(true);
  }

  private void verifyEvict(boolean useCompoundFile) throws IOException {
    initServers(useCompoundFile);
    indexAndReplicate(0);
    // check residency before searching, since rescoring reads the full precision vectors
    verifyReplicaResidency(useCompoundFile);
    // quantized search does not read the full precision vectors
    verifyReplicaSearch(0);
    verifyReplicaResidency(useCompoundFile);
    // rescoring reads full precision vectors on demand, and returns correct results
    verifyReplicaSearch(3.0f);

    // segments copied for a later nrt point are also evicted
    indexAndReplicate(NUM_DOCS);
    verifyReplicaSearch(0);
    verifyReplicaResidency(useCompoundFile, newSegmentFiles());
  }

  @Test
  public void testReplicaHasApplier() throws IOException {
    initServers(true);
    PageCachePolicyApplier applier = replicaShard().nrtReplicaNode.getPageCachePolicyApplier();
    assertEquals(PageCacheUtils.isSupported(), applier != PageCachePolicyApplier.NOOP);
  }

  private void initServers(boolean useCompoundFile) throws IOException {
    primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.REMOTE)
            .withAdditionalConfig(EVICT_CONFIG)
            .build();
    primaryServer.createIndex(INDEX);
    primaryServer.registerFields(
        INDEX,
        List.of(
            Field.newBuilder()
                .setName("id")
                .setType(FieldType.ATOM)
                .setStoreDocValues(true)
                .build(),
            Field.newBuilder()
                .setName(FIELD)
                .setType(FieldType.VECTOR)
                .setSearch(true)
                .setVectorDimensions(DIMENSIONS)
                .setVectorSimilarity("dot_product")
                .setVectorIndexingOptions(
                    VectorIndexingOptions.newBuilder().setType("hnsw_scalar_quantized").build())
                .build()));
    primaryServer
        .getClient()
        .getBlockingStub()
        .liveSettingsV2(
            LiveSettingsV2Request.newBuilder()
                .setIndexName(INDEX)
                .setLiveSettings(
                    IndexLiveSettings.newBuilder()
                        .setUseCompoundFile(BoolValue.of(useCompoundFile))
                        .build())
                .build());
    primaryServer.startPrimaryIndex(INDEX, -1, null);

    replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexStartConfig.IndexDataLocationType.REMOTE)
            .withAdditionalConfig(EVICT_CONFIG)
            .build();
    replicaServer.registerWithPrimary(INDEX);
  }

  private Set<String> segmentFilesBefore = Set.of();

  private Set<String> newSegmentFiles() throws IOException {
    Set<String> files = new HashSet<>(Arrays.asList(replicaShard().indexDir.listAll()));
    files.removeAll(segmentFilesBefore);
    return files;
  }

  private void indexAndReplicate(int startId) throws IOException {
    if (replicaServer != null && startId > 0) {
      segmentFilesBefore = new HashSet<>(Arrays.asList(replicaShard().indexDir.listAll()));
    }
    Random random = new Random(42 + startId);
    List<AddDocumentRequest> requests = new ArrayList<>();
    for (int i = startId; i < startId + NUM_DOCS; ++i) {
      requests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(INDEX)
              .putFields(
                  "id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("" + i).build())
              .putFields(
                  FIELD,
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(vectorString(randomVector(random)))
                      .build())
              .build());
    }
    primaryServer.addDocs(requests.stream());
    primaryServer.refresh(INDEX);
    replicaServer.waitForReplication(INDEX, primaryServer);
  }

  private void verifyReplicaSearch(float oversample) {
    List<Float> queryVector = new ArrayList<>();
    for (float value : randomVector(new Random(7))) {
      queryVector.add(value);
    }
    KnnQuery.Builder knnQuery =
        KnnQuery.newBuilder()
            .setField(FIELD)
            .addAllQueryVector(queryVector)
            .setNumCandidates(50)
            .setK(10);
    if (oversample > 0) {
      knnQuery.setRescoreVector(RescoreVector.newBuilder().setOversample(oversample));
    }
    SearchResponse response =
        replicaServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .addRetrieveFields("id")
                    .setTopHits(10)
                    .addKnn(knnQuery.build())
                    .build());
    assertEquals("oversample: " + oversample, 10, response.getHitsCount());
    for (int i = 1; i < response.getHitsCount(); ++i) {
      assertTrue(response.getHits(i - 1).getScore() >= response.getHits(i).getScore());
    }
  }

  private void verifyReplicaResidency(boolean useCompoundFile) throws IOException {
    verifyReplicaResidency(
        useCompoundFile, new HashSet<>(Arrays.asList(replicaShard().indexDir.listAll())));
  }

  private void verifyReplicaResidency(boolean useCompoundFile, Set<String> files)
      throws IOException {
    if (!PageCacheUtils.isSupported()) {
      return;
    }
    ShardState shard = replicaShard();
    Path indexPath = ((FSDirectory) shard.origIndexDir).getDirectory();
    long pageSize = PageCacheUtils.getPageSize();
    int coldRanges = 0;
    for (String file : files) {
      if (useCompoundFile && CompoundFileRanges.isCompoundDataFile(file)) {
        Map<String, FileRange> subFiles = CompoundFileRanges.read(shard.indexDir, file);
        for (Map.Entry<String, FileRange> entry : subFiles.entrySet()) {
          if (entry.getKey().endsWith(".vec") && entry.getKey().contains("Quantized")) {
            // at most the partial pages at the edges of the range remain
            assertTrue(
                PageCacheUtils.residentPages(indexPath.resolve(file), entry.getValue()) <= 2);
            coldRanges++;
          }
        }
      } else if (!useCompoundFile && file.endsWith(".vec") && file.contains("Quantized")) {
        long length = shard.indexDir.fileLength(file);
        assertTrue(length > 4 * pageSize);
        // opening the file reads the header and footer pages, but no vector data
        assertTrue(
            PageCacheUtils.residentPages(indexPath.resolve(file), new FileRange(0, length)) <= 2);
        FileRange vectorData = new FileRange(pageSize, length - 2 * pageSize);
        assertEquals(0, PageCacheUtils.residentPages(indexPath.resolve(file), vectorData));
        coldRanges++;
      }
    }
    assertTrue(coldRanges > 0);
  }

  private ShardState replicaShard() throws IOException {
    return replicaServer.getGlobalState().getIndexOrThrow(INDEX).getShard(0);
  }

  private static float[] randomVector(Random random) {
    float[] vector = new float[DIMENSIONS];
    float norm = 0;
    for (int i = 0; i < DIMENSIONS; ++i) {
      vector[i] = random.nextFloat() - 0.5f;
      norm += vector[i] * vector[i];
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < DIMENSIONS; ++i) {
      vector[i] /= norm;
    }
    return vector;
  }

  private static String vectorString(float[] vector) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < vector.length; ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(vector[i]);
    }
    return sb.append("]").toString();
  }
}
