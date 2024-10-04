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
import com.yelp.nrtsearch.server.field.properties.PolygonQueryable;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.GeoPointQuery;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.*;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class PolygonfieldDef extends IndexableFieldDef<Struct> implements PolygonQueryable {

  protected PolygonfieldDef(String name, Field requestField) {
    super(name, requestField, Struct.class);
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
    if (hasDocValues()) {
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
}
