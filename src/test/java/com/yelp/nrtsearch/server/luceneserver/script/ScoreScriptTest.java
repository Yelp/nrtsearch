/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.script;

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.primitives.Floats;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.GrpcServer;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues.SingleVector;
import com.yelp.nrtsearch.server.luceneserver.geo.GeoPoint;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ScoreScriptTest {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private GrpcServer grpcServer;
  private CollectorRegistry collectorRegistry;

  @After
  public void tearDown() throws IOException {
    tearDownGrpcServer();
  }

  private void tearDownGrpcServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.shutdown();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws IOException {
    collectorRegistry = new CollectorRegistry();
    grpcServer = setUpGrpcServer(collectorRegistry);
  }

  private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    return new GrpcServer(
        collectorRegistry,
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        null,
        luceneServerConfiguration.getIndexDir(),
        testIndex,
        luceneServerConfiguration.getPort(),
        null,
        Collections.singletonList(new ScoreScriptTestPlugin()));
  }

  static class ScoreScriptTestPlugin extends Plugin implements ScriptPlugin {
    @Override
    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Collections.singletonList(new TestScriptEngine());
    }
  }

  static class TestScriptEngine implements ScriptEngine {
    @Override
    public String getLang() {
      return "test_lang";
    }

    @Override
    public <T> T compile(String source, ScriptContext<T> context) {
      ScoreScript.Factory factory =
          ((params, docLookup) -> new TestScriptFactory(params, docLookup, source));
      return context.factoryClazz.cast(factory);
    }
  }

  static class TestScriptFactory extends ScoreScript.SegmentFactory {
    private final Map<String, Object> params;
    private final DocLookup docLookup;
    private final String scriptId;

    public TestScriptFactory(Map<String, Object> params, DocLookup docLookup, String scriptId) {
      super(params, docLookup);
      this.params = params;
      this.docLookup = docLookup;
      this.scriptId = scriptId;
    }

    @Override
    public boolean needs_score() {
      return true;
    }

    @Override
    public DoubleValues newInstance(LeafReaderContext ctx, DoubleValues scores) {
      switch (scriptId) {
        case "verify_doc_values":
          return new VerifyDocValuesScript(params, docLookup, ctx, scores);
        case "verify_lat_lon":
          return new VerifyLatLonScript(params, docLookup, ctx, scores);
        case "verify_score":
          return new VerifyScoreScript(params, docLookup, ctx, scores);
        case "verify_empty_doc_values":
          return new VerifyEmptyDocValuesScript(params, docLookup, ctx, scores);
        case "verify_empty_lat_lon_values":
          return new VerifyEmptyLatLonValuesScript(params, docLookup, ctx, scores);
        case "doc_values_errors":
          return new DocValuesExceptionsScript(params, docLookup, ctx, scores);
        case "test_no_params":
          return new TestNoParamsScript(params, docLookup, ctx, scores);
        case "test_params":
          return new TestParamsScript(params, docLookup, ctx, scores);
        case "verify_vector_type_doc_values":
          return new VerifyVectorTypeScript(params, docLookup, ctx, scores);
      }
      throw new IllegalArgumentException("Unknown script id: " + scriptId);
    }
  }

  static class VerifyDocValuesScript extends ScoreScript {
    public VerifyDocValuesScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        LoadedDocValues<?> idDocValues = getDoc().get("doc_id");
        assertEquals("doc_id size", 1, idDocValues.size());
        assertEquals("doc_id class", LoadedDocValues.SingleString.class, idDocValues.getClass());
        String id = ((LoadedDocValues.SingleString) idDocValues).get(0);

        List<Integer> expectedLicenseNo = null;
        List<Integer> expectedCount = null;
        List<String> expectedVendorName = null;
        List<String> expectedVendorNameAtom = null;
        List<String> expectedDescription = null;
        List<Double> expectedDoubleFieldMulti = null;
        List<Double> expectedDoubleField = null;
        List<Float> expectedFloatFieldMulti = null;
        List<Float> expectedFloatField = null;
        List<Boolean> expectedBooleanFieldMulti = Arrays.asList(false, true);
        List<Boolean> expectedBooleanField = Collections.singletonList(false);
        List<Long> expectedLongFieldMulti = null;
        List<Long> expectedLongField = null;
        List<Instant> expectedDate = null;
        List<Instant> expectedDateMulti = null;

        if (id.equals("1")) {
          expectedLicenseNo = Arrays.asList(300, 3100);
          expectedCount = Collections.singletonList(3);
          expectedVendorName = Arrays.asList("first again", "first vendor");
          expectedVendorNameAtom = Arrays.asList("first atom again", "first atom vendor");
          expectedDescription = Collections.singletonList("FIRST food");
          expectedDoubleFieldMulti = Arrays.asList(1.1, 1.11);
          expectedDoubleField = Collections.singletonList(1.01);
          expectedFloatFieldMulti = Arrays.asList(100.1F, 100.11F);
          expectedFloatField = Collections.singletonList(100.01F);
          expectedLongFieldMulti = Arrays.asList(1L, 2L);
          expectedLongField = Collections.singletonList(12L);
          expectedDate =
              Collections.singletonList(getStringDateTimeAsInstant("2019-10-12 15:30:41"));
          expectedDateMulti =
              Arrays.asList(
                  getStringDateTimeAsInstant("2019-11-13 10:20:15"),
                  getStringDateTimeAsInstant("2019-12-14 08:40:45"));
        } else if (id.equals("2")) {
          expectedLicenseNo = Arrays.asList(411, 4222);
          expectedCount = Collections.singletonList(7);
          expectedVendorName = Arrays.asList("second again", "second vendor");
          expectedVendorNameAtom = Arrays.asList("second atom again", "second atom vendor");
          expectedDescription = Collections.singletonList("SECOND gas");
          expectedDoubleFieldMulti = Arrays.asList(2.2, 2.22);
          expectedDoubleField = Collections.singletonList(2.01);
          expectedFloatFieldMulti = Arrays.asList(200.2F, 200.22F);
          expectedFloatField = Collections.singletonList(200.02F);
          expectedLongFieldMulti = Arrays.asList(3L, 4L);
          expectedLongField = Collections.singletonList(16L);
          expectedDate =
              Collections.singletonList(getStringDateTimeAsInstant("2020-03-05 01:03:05"));
          expectedDateMulti =
              Arrays.asList(
                  getStringDateTimeAsInstant("2018-01-20 00:31:00"),
                  getStringDateTimeAsInstant("2018-02-21 09:43:30"));
        } else {
          fail(String.format("docId %s not indexed", id));
        }

        assertDocValue(
            "license_no",
            LoadedDocValues.SortedIntegers.class,
            Integer.class,
            expectedLicenseNo,
            getDoc());
        assertDocValue(
            "count", LoadedDocValues.SingleInteger.class, Integer.class, expectedCount, getDoc());
        assertDocValue(
            "vendor_name",
            LoadedDocValues.SortedStrings.class,
            String.class,
            expectedVendorName,
            getDoc());
        assertDocValue(
            "vendor_name_atom",
            LoadedDocValues.SortedStrings.class,
            String.class,
            expectedVendorNameAtom,
            getDoc());
        assertDocValue(
            "description",
            LoadedDocValues.SortedStrings.class,
            String.class,
            expectedDescription,
            getDoc());
        assertDocValue(
            "double_field_multi",
            LoadedDocValues.SortedDoubles.class,
            Double.class,
            expectedDoubleFieldMulti,
            getDoc());
        assertDocValue(
            "double_field",
            LoadedDocValues.SingleDouble.class,
            Double.class,
            expectedDoubleField,
            getDoc());
        assertDocValue(
            "float_field_multi",
            LoadedDocValues.SortedFloats.class,
            Float.class,
            expectedFloatFieldMulti,
            getDoc());
        assertDocValue(
            "float_field",
            LoadedDocValues.SingleFloat.class,
            Float.class,
            expectedFloatField,
            getDoc());
        assertDocValue(
            "long_field_multi",
            LoadedDocValues.SortedLongs.class,
            Long.class,
            expectedLongFieldMulti,
            getDoc());
        assertDocValue(
            "long_field",
            LoadedDocValues.SingleLong.class,
            Long.class,
            expectedLongField,
            getDoc());
        assertDocValue(
            "boolean_field_multi",
            LoadedDocValues.SortedBooleans.class,
            Boolean.class,
            expectedBooleanFieldMulti,
            getDoc());
        assertDocValue(
            "boolean_field",
            LoadedDocValues.SingleBoolean.class,
            Boolean.class,
            expectedBooleanField,
            getDoc());
        assertDocValue(
            "date", LoadedDocValues.SingleDateTime.class, Instant.class, expectedDate, getDoc());
        assertDocValue(
            "date_multi",
            LoadedDocValues.SortedDateTimes.class,
            Instant.class,
            expectedDateMulti,
            getDoc());
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 1.5;
    }
  }

  static class VerifyLatLonScript extends ScoreScript {
    public VerifyLatLonScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        LoadedDocValues<?> idDocValues = getDoc().get("doc_id");
        assertEquals("doc_id size", 1, idDocValues.size());
        assertEquals("doc_id class", LoadedDocValues.SingleString.class, idDocValues.getClass());
        String id = ((LoadedDocValues.SingleString) idDocValues).get(0);

        List<Integer> expectedLicenseNo = null;
        List<String> expectedVendorName = null;
        List<String> expectedVendorNameAtom = null;
        GeoPoint expectedPoint = null;
        List<GeoPoint> expectedMultiPoints = null;

        if (id.equals("1")) {
          expectedLicenseNo = Arrays.asList(300, 3100);
          expectedVendorName = Arrays.asList("first again", "first vendor");
          expectedVendorNameAtom = Arrays.asList("first atom again", "first atom vendor");
          expectedPoint = new GeoPoint(37.7749, -122.393990);
          expectedMultiPoints =
              Arrays.asList(new GeoPoint(30.9988, -120.33977), new GeoPoint(40.1748, -142.453490));
        } else if (id.equals("2")) {
          expectedLicenseNo = Arrays.asList(411, 4222);
          expectedVendorName = Arrays.asList("second again", "second vendor");
          expectedVendorNameAtom = Arrays.asList("second atom again", "second atom vendor");
          expectedPoint = new GeoPoint(37.5485, -121.9886);
          expectedMultiPoints =
              Arrays.asList(new GeoPoint(29.9988, -119.33977), new GeoPoint(39.1748, -141.453490));
        } else {
          fail(String.format("docId %s not indexed", id));
        }

        assertDocValue(
            "license_no",
            LoadedDocValues.SortedIntegers.class,
            Integer.class,
            expectedLicenseNo,
            getDoc());
        assertDocValue(
            "vendor_name",
            LoadedDocValues.SortedStrings.class,
            String.class,
            expectedVendorName,
            getDoc());
        assertDocValue(
            "vendor_name_atom",
            LoadedDocValues.SortedStrings.class,
            String.class,
            expectedVendorNameAtom,
            getDoc());

        String fieldName = "lat_lon";
        LoadedDocValues<?> docValues = getDoc().get(fieldName);
        assertNotNull(fieldName + " is null", docValues);
        assertEquals(LoadedDocValues.SingleLocation.class, docValues.getClass());
        LoadedDocValues.SingleLocation singleLocation = (LoadedDocValues.SingleLocation) docValues;
        assertEquals(fieldName + " size", 1, singleLocation.size());
        GeoPoint loadedPoint = singleLocation.get(0);
        assertNotNull("point is null", loadedPoint);
        assertEquals(fieldName + " latitude", expectedPoint.getLat(), loadedPoint.getLat(), 0.0001);
        assertEquals(
            fieldName + " longitude", expectedPoint.getLon(), loadedPoint.getLon(), 0.0001);
        assertEquals(
            fieldName + " latitude (left)",
            expectedPoint.getLat(),
            loadedPoint.leftDouble(),
            0.0001);
        assertEquals(
            fieldName + " longitude (right)",
            expectedPoint.getLon(),
            loadedPoint.rightDouble(),
            0.0001);

        fieldName = "lat_lon_multi";
        docValues = getDoc().get(fieldName);
        assertNotNull(fieldName + " is null", docValues);
        assertEquals(LoadedDocValues.Locations.class, docValues.getClass());
        LoadedDocValues.Locations locations = (LoadedDocValues.Locations) docValues;
        assertEquals(fieldName + " size", 2, locations.size());
        for (int i = 0; i < locations.size(); ++i) {
          loadedPoint = locations.get(i);
          assertNotNull("point " + i + " is null", loadedPoint);
          assertEquals(
              fieldName + " latitude " + i,
              expectedMultiPoints.get(i).getLat(),
              loadedPoint.getLat(),
              0.0001);
          assertEquals(
              fieldName + " longitude " + i,
              expectedMultiPoints.get(i).getLon(),
              loadedPoint.getLon(),
              0.0001);
        }
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 1.75;
    }
  }

  static class VerifyScoreScript extends ScoreScript {
    public VerifyScoreScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        LoadedDocValues<?> idDocValues = getDoc().get("doc_id");
        assertEquals("doc_id size", 1, idDocValues.size());
        assertEquals("doc_id class", LoadedDocValues.SingleString.class, idDocValues.getClass());
        String id = ((LoadedDocValues.SingleString) idDocValues).get(0);

        if (id.equals("1")) {
          assertEquals(0.516, get_score(), 0.001);
        } else if (id.equals("2")) {
          assertEquals(0.0828, get_score(), 0.001);
        } else {
          fail(String.format("docId %s not indexed", id));
        }
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 2.0;
    }
  }

  static class VerifyEmptyDocValuesScript extends ScoreScript {
    public VerifyEmptyDocValuesScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        LoadedDocValues<?> idDocValues = getDoc().get("doc_id");
        assertEquals("doc_id size", 1, idDocValues.size());
        assertEquals("doc_id class", LoadedDocValues.SingleString.class, idDocValues.getClass());
        assertEquals("doc_id exists", 1, idDocValues.size());

        assertEmptyDocValues("license_no", getDoc());
        assertEmptyDocValues("vendor_name", getDoc());
        assertEmptyDocValues("vendor_name_atom", getDoc());
        assertEmptyDocValues("count", getDoc());
        assertEmptyDocValues("long_field_multi", getDoc());
        assertEmptyDocValues("long_field", getDoc());
        assertEmptyDocValues("double_field_multi", getDoc());
        assertEmptyDocValues("double_field", getDoc());
        assertEmptyDocValues("float_field_multi", getDoc());
        assertEmptyDocValues("float_field", getDoc());
        assertEmptyDocValues("boolean_field_multi", getDoc());
        assertEmptyDocValues("boolean_field", getDoc());
        assertEmptyDocValues("date_multi", getDoc());
        assertEmptyDocValues("date", getDoc());
        assertEmptyDocValues("description", getDoc());
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 2.5;
    }
  }

  static class VerifyEmptyLatLonValuesScript extends ScoreScript {
    public VerifyEmptyLatLonValuesScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        LoadedDocValues<?> idDocValues = getDoc().get("doc_id");
        assertEquals("doc_id size", 1, idDocValues.size());
        assertEquals("doc_id class", LoadedDocValues.SingleString.class, idDocValues.getClass());
        assertEquals("doc_id exists", 1, idDocValues.size());

        assertEmptyDocValues("license_no", getDoc());
        assertEmptyDocValues("vendor_name", getDoc());
        assertEmptyDocValues("vendor_name_atom", getDoc());
        assertEmptyDocValues("lat_lon", getDoc());
        assertEmptyDocValues("lat_lon_multi", getDoc());
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 2.5;
    }
  }

  static class DocValuesExceptionsScript extends ScoreScript {
    public DocValuesExceptionsScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        LoadedDocValues<?> idDocValues = getDoc().get("doc_id");
        assertEquals("doc_id size", 1, idDocValues.size());
        assertEquals("doc_id class", LoadedDocValues.SingleString.class, idDocValues.getClass());
        assertEquals("doc_id exists", 1, idDocValues.size());

        try {
          getDoc().get(null);
          fail("null field name");
        } catch (NullPointerException ignore) {
        }
        try {
          getDoc().get("not_field");
          fail("Invalid field");
        } catch (IllegalArgumentException ignore) {
        }
        LoadedDocValues<?> docValues = getDoc().get("long_field");
        try {
          docValues.get(2);
          fail("Invaild array index");
        } catch (IndexOutOfBoundsException ignore) {
        }
        docValues = getDoc().get("long_field_multi");
        try {
          docValues.get(2);
          fail("Invaild array index");
        } catch (IndexOutOfBoundsException ignore) {
        }
        docValues = getDoc().get("vendor_name_atom");
        try {
          docValues.get(2);
          fail("Invaild array index");
        } catch (IndexOutOfBoundsException ignore) {
        }
        docValues = getDoc().get("vendor_name");
        try {
          docValues.get(2);
          fail("Invaild array index");
        } catch (IndexOutOfBoundsException ignore) {
        }
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 3.5;
    }
  }

  static class TestNoParamsScript extends ScoreScript {
    public TestNoParamsScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        assertNotNull("params is null", getParams());
        assertEquals("params size", 0, getParams().size());
        assertNull("param should be null", getParams().get("not_param"));
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 4.0;
    }
  }

  static class TestParamsScript extends ScoreScript {
    public TestParamsScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        assertNotNull("params is null", getParams());
        assertEquals("params size", 6, getParams().size());
        assertParam("text_param", String.class, "text_val");
        assertParam("bool_param", Boolean.class, false);
        assertParam("int_param", Integer.class, 100);
        assertParam("long_param", Long.class, 1001L);
        assertParam("float_param", Float.class, 1.123F);
        assertParam("double_param", Double.class, 3.456);
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 4.5;
    }

    private <T> void assertParam(String name, Class<T> clazz, T expectedValue) {
      assertEquals(name + " type", clazz, getParams().get(name).getClass());
      assertEquals(name + " value", expectedValue, getParams().get(name));
    }
  }

  static class VerifyVectorTypeScript extends ScoreScript {

    public VerifyVectorTypeScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      try {
        String vectorFieldName = "vector_field";
        float[] expectedVector1 = {0.1f, 0.2f, 0.3f};
        float[] expectedVector2 = {0.0f, 1.0f, 0.0f};

        LoadedDocValues<?> idDocValues = getDoc().get("doc_id");
        assertEquals("doc_id size", 1, idDocValues.size());
        assertEquals("doc_id class", LoadedDocValues.SingleString.class, idDocValues.getClass());
        String id = ((LoadedDocValues.SingleString) idDocValues).get(0);

        LoadedDocValues<?> docValues = getDoc().get(vectorFieldName);
        assertNotNull(vectorFieldName + " is null", docValues);
        assertEquals(SingleVector.class, docValues.getClass());

        SingleVector vectorDocValues = (SingleVector) docValues;
        float[] loadedVectorValue = vectorDocValues.get(0).getVectorData();
        List<Float> vectorFieldValues =
            vectorDocValues.toFieldValue(0).getVectorValue().getValueList();

        if (id.equals("1")) {
          assertTrue(Arrays.equals(expectedVector1, loadedVectorValue));
          assertEquals(Floats.asList(expectedVector1), vectorFieldValues);
        } else if (id.equals("2")) {
          assertTrue(Arrays.equals(expectedVector2, loadedVectorValue));
          assertEquals(Floats.asList(expectedVector2), vectorFieldValues);
        }

        // Test IndexOutOfBoundsException
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> vectorDocValues.get(1));
      } catch (Error e) {
        throw new RuntimeException(e.getMessage(), e.getCause());
      }
      return 0;
    }
  }

  @Test
  public void testScriptDocValues() throws Exception {
    testQueryFieldScript("verify_doc_values", "registerFieldsBasic.json", "addDocs.csv", 1.5);
  }

  @Ignore("Only js scripting language is supported in index fields now, enable after fix")
  @Test
  public void testScriptDocValuesIndexField() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsScriptTest.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocs.csv");
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .addRetrieveFields("test_doc_values")
                    .setStartHit(0)
                    .setTopHits(10)
                    .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        1.5,
        searchResponse
            .getHits(0)
            .getFieldsOrThrow("test_doc_values")
            .getFieldValue(0)
            .getDoubleValue(),
        Math.ulp(1.5));
    assertEquals(
        1.5,
        searchResponse
            .getHits(1)
            .getFieldsOrThrow("test_doc_values")
            .getFieldValue(0)
            .getDoubleValue(),
        Math.ulp(1.5));
  }

  @Test
  public void testScriptDocValuesScoreQuery() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse = doFunctionScoreQuery("verify_doc_values");
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(1.5, searchResponse.getHits(0).getScore(), Math.ulp(1.5));
    assertEquals(1.5, searchResponse.getHits(1).getScore(), Math.ulp(1.5));
  }

  @Test
  public void testScriptLatLonDocValues() throws Exception {
    testQueryFieldScript("verify_lat_lon", "registerFieldsLatLon.json", "addDocsLatLon.csv", 1.75);
  }

  @Test
  public void testEmptyDocValues() throws Exception {
    testQueryFieldScript(
        "verify_empty_doc_values", "registerFieldsBasic.json", "addDocsEmpty.csv", 2.5);
  }

  @Test
  public void testEmptyLatLonDocValues() throws Exception {
    testQueryFieldScript(
        "verify_empty_lat_lon_values", "registerFieldsLatLon.json", "addDocsEmpty.csv", 2.5);
  }

  @Test
  public void testDocValuesExceptions() throws Exception {
    testQueryFieldScript("doc_values_errors", "registerFieldsBasic.json", "addDocs.csv", 3.5);
  }

  @Test
  public void testVectorTypeDocValues() throws Exception {
    testQueryFieldScript(
        "verify_vector_type_doc_values", "registerFieldsVector.json", "addDocsVectorType.csv", 0);
  }

  @Test
  public void testScriptUsingScore() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    VirtualField virtualField =
        VirtualField.newBuilder()
            .setName("test_field")
            .setScript(Script.newBuilder().setLang("test_lang").setSource("verify_score").build())
            .build();

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addVirtualFields(virtualField)
                    .addRetrieveFields("test_field")
                    .setQueryText("vendor_name:first vendor")
                    .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        2.0,
        searchResponse.getHits(0).getFieldsOrThrow("test_field").getFieldValue(0).getDoubleValue(),
        Math.ulp(2.0));
    assertEquals(
        2.0,
        searchResponse.getHits(1).getFieldsOrThrow("test_field").getFieldValue(0).getDoubleValue(),
        Math.ulp(2.0));
  }

  @Ignore("Only js scripting language is supported in index fields now, enable after fix")
  @Test
  public void testScriptUsingScoreInIndexField() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsScriptTest.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocs.csv");
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .addRetrieveFields("test_score")
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQueryText("vendor_name:first vendor")
                    .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        2.0,
        searchResponse.getHits(0).getFieldsOrThrow("test_score").getFieldValue(0).getDoubleValue(),
        Math.ulp(2.0));
    assertEquals(
        2.0,
        searchResponse.getHits(1).getFieldsOrThrow("test_score").getFieldValue(0).getDoubleValue(),
        Math.ulp(2.0));
  }

  @Test
  public void testScriptUsingScoreInScoreQuery() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse = doFunctionScoreQuery("verify_score");
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(2.0, searchResponse.getHits(0).getScore(), Math.ulp(2.0));
    assertEquals(2.0, searchResponse.getHits(1).getScore(), Math.ulp(2.0));
  }

  @Test
  public void testNoParams() throws Exception {
    testQueryFieldScript("test_no_params", "registerFieldsBasic.json", "addDocs.csv", 4.0);
  }

  @Test
  public void testParams() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    VirtualField virtualField =
        VirtualField.newBuilder()
            .setName("test_field")
            .setScript(
                Script.newBuilder()
                    .setLang("test_lang")
                    .setSource("test_params")
                    .putParams(
                        "text_param",
                        Script.ParamValue.newBuilder().setTextValue("text_val").build())
                    .putParams(
                        "bool_param", Script.ParamValue.newBuilder().setBooleanValue(false).build())
                    .putParams("int_param", Script.ParamValue.newBuilder().setIntValue(100).build())
                    .putParams(
                        "long_param", Script.ParamValue.newBuilder().setLongValue(1001).build())
                    .putParams(
                        "float_param", Script.ParamValue.newBuilder().setFloatValue(1.123F).build())
                    .putParams(
                        "double_param",
                        Script.ParamValue.newBuilder().setDoubleValue(3.456).build())
                    .build())
            .build();

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addVirtualFields(virtualField)
                    .addRetrieveFields("test_field")
                    .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        4.5,
        searchResponse.getHits(0).getFieldsOrThrow("test_field").getFieldValue(0).getDoubleValue(),
        Math.ulp(4.5));
    assertEquals(
        4.5,
        searchResponse.getHits(1).getFieldsOrThrow("test_field").getFieldValue(0).getDoubleValue(),
        Math.ulp(4.5));
  }

  private void testQueryFieldScript(
      String source, String registerFieldsFile, String addDocsFile, double expectedScore)
      throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, registerFieldsFile);
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments(addDocsFile);
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    VirtualField virtualField =
        VirtualField.newBuilder()
            .setName("test_field")
            .setScript(Script.newBuilder().setLang("test_lang").setSource(source).build())
            .build();

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addVirtualFields(virtualField)
                    .addRetrieveFields("test_field")
                    .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        expectedScore,
        searchResponse.getHits(0).getFieldsOrThrow("test_field").getFieldValue(0).getDoubleValue(),
        Math.ulp(expectedScore));
    assertEquals(
        expectedScore,
        searchResponse.getHits(1).getFieldsOrThrow("test_field").getFieldValue(0).getDoubleValue(),
        Math.ulp(expectedScore));
  }

  private SearchResponse doFunctionScoreQuery(String scriptSource) {
    return grpcServer
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .setQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("test_lang")
                                        .setSource(scriptSource)
                                        .build())
                                .setQuery(
                                    Query.newBuilder()
                                        .setMatchQuery(
                                            MatchQuery.newBuilder()
                                                .setField("vendor_name")
                                                .setQuery("first vendor")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());
  }

  private static Instant getStringDateTimeAsInstant(String dateTime) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDateTime.parse(dateTime, dateTimeFormatter).toInstant(ZoneOffset.UTC);
  }

  @SuppressWarnings("unchecked")
  private static <T, V> void assertDocValue(
      String fieldName,
      Class<T> docValueClass,
      Class<V> fieldValueClass,
      List<V> expectedValues,
      Map<String, LoadedDocValues<?>> doc) {
    LoadedDocValues<?> docValues = doc.get(fieldName);
    assertNotNull(fieldName + " is null", docValues);
    assertEquals(docValueClass, docValues.getClass());

    List<V> valuesList = new ArrayList<>();
    for (Object value : docValues) {
      assertEquals(fieldName + " value class", fieldValueClass, value.getClass());
      valuesList.add((V) value);
    }
    assertEquals(fieldName + " values", expectedValues, valuesList);
  }

  private static void assertEmptyDocValues(String fieldName, Map<String, LoadedDocValues<?>> doc) {
    assertTrue("doc contains " + fieldName, doc.containsKey(fieldName));
    LoadedDocValues<?> docValues = doc.get(fieldName);
    assertNotNull(fieldName + " doc value is null", docValues);
    assertEquals(fieldName + " doc value list size", 0, docValues.size());
    try {
      docValues.get(0);
      fail("no doc values");
    } catch (IllegalStateException ignored) {
    }
  }
}
