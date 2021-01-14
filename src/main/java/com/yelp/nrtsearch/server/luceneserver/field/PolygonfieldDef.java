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
import com.yelp.nrtsearch.server.grpc.GeoPointQuery;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.properties.PolygonQueryable;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;

public class PolygonfieldDef extends IndexableFieldDef implements PolygonQueryable {

  protected PolygonfieldDef(String name, Field requestField) {
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

    if (hasAnalyzer(requestField)) {
      throw new IllegalArgumentException("no analyzer allowed on polygon field");
    }
  }

  protected DocValuesType parseDocValuesType(Field requestField) {
    // ES does not support geoshape doc value
    // polygon can only be retrieved from _source in ES
    // TODO: figure out later for nrtsearch
    return DocValuesType.NONE;
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1) {
      throw new IllegalArgumentException("polygon length cannot be more than 1.");
    }
    for (String fieldValue : fieldValues) {
      /**
       * Map<String, Object> geojson = gson.fromJson(fieldValue, Map.class); if
       * (!geojson.get("type").equals("polygon")) { throw new IllegalArgumentException( "Non
       * supported types. Only polygon type is supported." ); } List<List<List<Double>>> coordinates
       * = (List<List<List<Double>>>) geojson.get("coordinates"); List<List<Double>> outer =
       * coordinates.get(0); List<List<List<Double>>> holes = coordinates.subList(1,
       * coordinates.size()); Polygon[] holesArray = (Polygon[]) ( holes.stream().map(hole ->
       * generatePolygon(hole, new Polygon[0])) ).toArray();
       */
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
          .forEach(x -> document.add(x));

      if (isStored()) {
        document.add(new StoredField(this.getName(), fieldValue));
      }
    }
  }

  /**
   * private Polygon generatePolygon(List<List<Double>> points, Polygon[] holes) { double[] lats =
   * new double[points.size()]; double[] lngs = new double[points.size()]; for (int i = 0; i <
   * points.size(); i++) { lngs[i] = points.get(i).get(0); lats[i] = points.get(i).get(1); } return
   * new Polygon(lats, lngs, holes); }
   */
  @Override
  public String getType() {
    return "POLYGON";
  }

  @Override
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    // TODO: figure out later
    return null;
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
}
