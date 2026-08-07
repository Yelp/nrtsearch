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
package com.yelp.nrtsearch.server.field;

import static com.yelp.nrtsearch.server.analysis.AnalyzerCreator.hasAnalyzer;

import com.google.protobuf.Struct;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.field.properties.GeoQueryable;
import com.yelp.nrtsearch.server.field.properties.PolygonQueryable;
import com.yelp.nrtsearch.server.geo.GeoUtils;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.GeoBoundingBoxQuery;
import com.yelp.nrtsearch.server.grpc.GeoPointQuery;
import com.yelp.nrtsearch.server.grpc.GeoPolygonQuery;
import com.yelp.nrtsearch.server.grpc.GeoRadiusQuery;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.*;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class PolygonfieldDef extends IndexableFieldDef<Struct>
    implements PolygonQueryable, GeoQueryable {

  protected PolygonfieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    this(name, requestField, context, null);
  }

  /**
   * Constructor for creating an instance of this field based on a previous instance. This is used
   * when updating field properties.
   *
   * @param name name of the field
   * @param requestField the field definition from the request
   * @param context context for creating the field definition
   * @param previousField the previous instance of this field definition, or null if there is none
   */
  protected PolygonfieldDef(
      String name,
      Field requestField,
      FieldDefCreator.FieldDefCreatorContext context,
      PolygonfieldDef previousField) {
    super(name, requestField, context, Struct.class, previousField);
  }

  @Override
  protected void validateRequest(Field requestField) {
    super.validateRequest(requestField);

    if (hasAnalyzer(requestField)) {
      throw new IllegalArgumentException("no analyzer allowed on polygon field");
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getStoreDocValues()) {
      return DocValuesType.BINARY;
    }
    return DocValuesType.NONE;
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1) {
      throw new IllegalArgumentException("polygon length cannot be more than 1.");
    }
    for (String fieldValue : fieldValues) {
      Polygon[] polygons;
      try {
        polygons = Polygon.fromGeoJSON(fieldValue);
      } catch (ParseException e) {
        throw new RuntimeException("Invalid geojson " + fieldValue + "\nException: " + e);
      }
      if (polygons.length > 1) {
        throw new IllegalArgumentException("Multipolygon not supported");
      }

      Arrays.stream(LatLonShape.createIndexableFields(getName(), polygons[0]))
          .forEach(document::add);

      if (isStored()) {
        document.add(
            new StoredField(this.getName(), ObjectFieldDef.jsonToStruct(fieldValue).toByteArray()));
      }
    }
    if (hasDocValues() && !fieldValues.isEmpty()) {
      document.add(
          new BinaryDocValuesField(
              getName(), new BytesRef(ObjectFieldDef.jsonToStructList(fieldValues).toByteArray())));
    }
  }

  @Override
  public String getType() {
    return "POLYGON";
  }

  @Override
  public FieldDef createUpdatedFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    return new PolygonfieldDef(name, requestField, context, this);
  }

  @Override
  public LoadedDocValues<Struct> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.BINARY) {
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new LoadedDocValues.ObjectStructDocValues(binaryDocValues);
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  public SearchResponse.Hit.FieldValue getStoredFieldValue(StoredValue value) {
    Struct struct = ObjectFieldDef.bytesRefToStruct(value.getBinaryValue());
    return SearchResponse.Hit.FieldValue.newBuilder().setStructValue(struct).build();
  }

  @Override
  public Query getGeoPointQuery(GeoPointQuery geoPointQuery) {
    return LatLonShape.newBoxQuery(
        geoPointQuery.getField(),
        ShapeField.QueryRelation.CONTAINS,
        geoPointQuery.getPoint().getLatitude(),
        geoPointQuery.getPoint().getLatitude(),
        geoPointQuery.getPoint().getLongitude(),
        geoPointQuery.getPoint().getLongitude());
  }

  @Override
  public Query getGeoRadiusQuery(GeoRadiusQuery geoRadiusQuery) {
    if (!this.isSearchable()) {
      throw new IllegalArgumentException(
          String.format("field %s is not searchable", this.getName()));
    }
    double radius = GeoUtils.getDistance(geoRadiusQuery.getRadius());
    return LatLonShape.newDistanceQuery(
        geoRadiusQuery.getField(),
        ShapeField.QueryRelation.INTERSECTS,
        new Circle(
            geoRadiusQuery.getCenter().getLatitude(),
            geoRadiusQuery.getCenter().getLongitude(),
            radius));
  }

  @Override
  public Query getGeoBoundingBoxQuery(GeoBoundingBoxQuery geoBoundingBoxQuery) {
    if (!this.isSearchable()) {
      throw new IllegalArgumentException(
          String.format("field %s is not searchable", this.getName()));
    }
    return LatLonShape.newBoxQuery(
        geoBoundingBoxQuery.getField(),
        ShapeField.QueryRelation.INTERSECTS,
        geoBoundingBoxQuery.getBottomRight().getLatitude(),
        geoBoundingBoxQuery.getTopLeft().getLatitude(),
        geoBoundingBoxQuery.getTopLeft().getLongitude(),
        geoBoundingBoxQuery.getBottomRight().getLongitude());
  }

  @Override
  public Query getGeoPolygonQuery(GeoPolygonQuery geoPolygonQuery) {
    if (!this.isSearchable()) {
      throw new IllegalArgumentException(
          String.format("field %s is not searchable", this.getName()));
    }
    if (geoPolygonQuery.getPolygonsCount() == 0) {
      throw new IllegalArgumentException("GeoPolygonQuery must contain at least one polygon");
    }
    Polygon[] polygons = new Polygon[geoPolygonQuery.getPolygonsCount()];
    for (int i = 0; i < geoPolygonQuery.getPolygonsCount(); ++i) {
      polygons[i] = toLucenePolygon(geoPolygonQuery.getPolygons(i));
    }
    return LatLonShape.newPolygonQuery(
        geoPolygonQuery.getField(), ShapeField.QueryRelation.INTERSECTS, polygons);
  }

  private static Polygon toLucenePolygon(com.yelp.nrtsearch.server.grpc.Polygon grpcPolygon) {
    int pointsCount = grpcPolygon.getPointsCount();
    if (pointsCount < 3) {
      throw new IllegalArgumentException("Polygon must have at least three points");
    }

    boolean closedShape =
        grpcPolygon.getPoints(0).equals(grpcPolygon.getPoints(grpcPolygon.getPointsCount() - 1));
    int pointsArraySize;
    if (closedShape) {
      if (pointsCount < 4) {
        throw new IllegalArgumentException("Closed Polygon must have at least four points");
      }
      pointsArraySize = pointsCount;
    } else {
      pointsArraySize = pointsCount + 1;
    }

    double[] latValues = new double[pointsArraySize];
    double[] lonValues = new double[pointsArraySize];
    for (int i = 0; i < grpcPolygon.getPointsCount(); ++i) {
      latValues[i] = grpcPolygon.getPoints(i).getLatitude();
      lonValues[i] = grpcPolygon.getPoints(i).getLongitude();
    }

    if (!closedShape) {
      latValues[pointsCount] = grpcPolygon.getPoints(0).getLatitude();
      lonValues[pointsCount] = grpcPolygon.getPoints(0).getLongitude();
    }

    Polygon[] holes = new Polygon[grpcPolygon.getHolesCount()];
    for (int i = 0; i < grpcPolygon.getHolesCount(); ++i) {
      holes[i] = toLucenePolygon(grpcPolygon.getHoles(i));
    }
    return new Polygon(latValues, lonValues, holes);
  }
}
