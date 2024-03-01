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
package com.yelp.nrtsearch.server.luceneserver.field;

import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.hasAnalyzer;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.GeoBoundingBoxQuery;
import com.yelp.nrtsearch.server.grpc.GeoPolygonQuery;
import com.yelp.nrtsearch.server.grpc.GeoRadiusQuery;
import com.yelp.nrtsearch.server.grpc.Point;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GeoQueryable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import com.yelp.nrtsearch.server.luceneserver.geo.GeoUtils;
import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/** Field class for 'LAT_LON' field type. */
public class LatLonFieldDef extends IndexableFieldDef implements Sortable, GeoQueryable {
  public LatLonFieldDef(String name, Field requestField) {
    super(name, requestField);
  }

  @Override
  protected void validateRequest(Field requestField) {
    super.validateRequest(requestField);

    if (requestField.getHighlight()) {
      throw new IllegalArgumentException(
          String.format(
              "field: %s cannot have highlight=true. only type=text or type=atom fields can have highlight=true",
              getName()));
    }

    if (requestField.getStore()) {
      throw new IllegalArgumentException("latlon fields cannot be stored");
    }

    if (hasAnalyzer(requestField)) {
      throw new IllegalArgumentException("no analyzer allowed on latlon field");
    }
  }

  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getSort() || requestField.getStoreDocValues()) {
      return DocValuesType.SORTED_NUMERIC;
    }
    return DocValuesType.NONE;
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    fieldType.setDimensions(2, Integer.BYTES);
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if ((fieldValues.size() % 2) == 1) {
      throw new IllegalArgumentException(
          "Odd number of values to index. lat_lon fields must be specified as a sequence of lat lon.");
    }
    if (!isMultiValue() && fieldValues.size() > 2) {
      throw new IllegalArgumentException("Trying to index multiple values into single value point");
    }
    for (int i = 0; i < fieldValues.size(); i = i + 2) {
      double latitude = Double.parseDouble(fieldValues.get(i));
      double longitude = Double.parseDouble(fieldValues.get(i + 1));
      if (hasDocValues()) {
        document.add(new LatLonDocValuesField(getName(), latitude, longitude));
      }
      if (isSearchable()) {
        document.add(new LatLonPoint(getName(), latitude, longitude));
      }
    }
  }

  @Override
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      SortedNumericDocValues sortedNumericDocValues =
          DocValues.getSortedNumeric(context.reader(), getName());
      if (isMultiValue()) {
        return new LoadedDocValues.Locations(sortedNumericDocValues);
      } else {
        return new LoadedDocValues.SingleLocation(sortedNumericDocValues);
      }
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  public String getType() {
    return "LAT_LON";
  }

  @Override
  public SortField getSortField(SortType type) {
    if (!hasDocValues()) {
      throw new IllegalStateException("Doc values are required for sorted fields");
    }
    Point origin = type.getOrigin();
    return LatLonDocValuesField.newDistanceSort(
        getName(), origin.getLatitude(), origin.getLongitude());
  }

  @Override
  public Query getGeoBoundingBoxQuery(GeoBoundingBoxQuery geoBoundingBoxQuery) {
    if (!this.isSearchable()) {
      throw new IllegalArgumentException(
          String.format("field %s is not searchable", this.getName()));
    }
    return LatLonPoint.newBoxQuery(
        geoBoundingBoxQuery.getField(),
        geoBoundingBoxQuery.getBottomRight().getLatitude(),
        geoBoundingBoxQuery.getTopLeft().getLatitude(),
        geoBoundingBoxQuery.getTopLeft().getLongitude(),
        geoBoundingBoxQuery.getBottomRight().getLongitude());
  }

  @Override
  public Query getGeoRadiusQuery(GeoRadiusQuery geoRadiusQuery) {
    if (!this.isSearchable()) {
      throw new IllegalArgumentException(
          String.format("field %s is not searchable", this.getName()));
    }
    double radius = GeoUtils.getDistance(geoRadiusQuery.getRadius());
    return LatLonPoint.newDistanceQuery(
        geoRadiusQuery.getField(),
        geoRadiusQuery.getCenter().getLatitude(),
        geoRadiusQuery.getCenter().getLongitude(),
        radius);
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
    return LatLonPoint.newPolygonQuery(geoPolygonQuery.getField(), polygons);
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

    // The first point is also used as the last point to create a closed shape
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

  @Override
  public BiFunction<SortField, Object, CompositeFieldValue> sortValueExtractor(SortType sortType) {
    double multiplier = GeoUtils.convertDistanceToADifferentUnit(1.0, sortType.getUnit());
    return (sortField, value) ->
        CompositeFieldValue.newBuilder()
            .addFieldValue(FieldValue.newBuilder().setDoubleValue(multiplier * (double) value))
            .build();
  }
}
