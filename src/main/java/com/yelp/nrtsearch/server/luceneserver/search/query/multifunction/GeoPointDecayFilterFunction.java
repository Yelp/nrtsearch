/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.query.multifunction;

import com.google.type.LatLng;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.doc.SegmentDocLookup;
import com.yelp.nrtsearch.server.luceneserver.field.LatLonFieldDef;
import com.yelp.nrtsearch.server.luceneserver.geo.GeoPoint;
import com.yelp.nrtsearch.server.luceneserver.geo.GeoUtils;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;

public class GeoPointDecayFilterFunction extends DecayFilterFunction {

  private final MultiFunctionScoreQuery.DecayFunction decayFunction;
  private final String fieldName;
  private final DecayFunction decayType;
  private final double scale;
  private final double offset;
  private final double decay;
  private final LatLng origin;
  private final IndexState indexState;

  /**
   * Constructor.
   *
   * @param filterQuery filter to use when applying this function, or null if none
   * @param weight weight multiple to scale the function score
   * @param decayFunction to score a document with a function that decays depending on the distance
   *     between an origin point and a numeric doc field value
   * @param indexState indexState for validation and doc value lookup
   */
  public GeoPointDecayFilterFunction(
      Query filterQuery,
      float weight,
      MultiFunctionScoreQuery.DecayFunction decayFunction,
      IndexState indexState) {
    super(filterQuery, weight, decayFunction);
    this.decayFunction = decayFunction;
    this.fieldName = decayFunction.getFieldName();
    this.decayType = getDecayType(decayFunction.getDecayType());
    this.offset =
        !decayFunction.getOffset().isEmpty()
            ? GeoUtils.getDistance(decayFunction.getOffset())
            : 0.0;
    this.decay = decayFunction.getDecay();
    double userGivenScale = GeoUtils.getDistance(decayFunction.getScale());
    this.scale = decayType.computeScale(userGivenScale, decay);
    this.indexState = indexState;
    if (!decayFunction.hasGeoPoint()) {
      throw new IllegalArgumentException("Decay Function should have a geoPoint for Origin field");
    } else {
      this.origin = decayFunction.getGeoPoint();
    }
    LatLonFieldDef latLonFieldDef = (LatLonFieldDef) indexState.getField(fieldName);
    // TODO: Add support for multi-value fields
    if (latLonFieldDef.isMultiValue()) {
      throw new IllegalArgumentException(
          "Multivalue fields are not supported for decay functions yet");
    }
  }

  @Override
  public LeafFunction getLeafFunction(LeafReaderContext leafContext) {
    return new GeoPointDecayLeafFunction(leafContext);
  }

  public final class GeoPointDecayLeafFunction implements LeafFunction {

    SegmentDocLookup segmentDocLookup;
    LatLng latLng;

    public GeoPointDecayLeafFunction(LeafReaderContext context) {
      segmentDocLookup = indexState.docLookup.getSegmentLookup(context);
    }

    @Override
    public double score(int docId, float innerQueryScore) throws IOException {
      segmentDocLookup.setDocId(docId);
      LoadedDocValues<GeoPoint> latLonValues =
          (LoadedDocValues<GeoPoint>) segmentDocLookup.get(fieldName);
      this.latLng = latLonValues.toFieldValue(0).getLatLngValue();
      double distance =
          GeoUtils.arcDistance(
              origin.getLatitude(),
              origin.getLongitude(),
              latLng.getLatitude(),
              latLng.getLongitude());
      double score = decayType.computeScore(distance, offset, scale);
      return score * getWeight();
    }

    @Override
    public Explanation explainScore(int docId, Explanation innerQueryScore) {
      double distance =
          GeoUtils.arcDistance(
              origin.getLatitude(),
              origin.getLongitude(),
              latLng.getLatitude(),
              latLng.getLongitude());
      double score = decayType.computeScore(distance, offset, scale);
      Explanation distanceExp =
          Explanation.match(distance, "arc distance calculated between two geoPoints");
      return Explanation.match(
          score,
          "final score with the provided decay function calculated by score * weight with "
              + getWeight()
              + " weight value",
          List.of(distanceExp, decayType.explainComputeScore(distance, offset, scale)));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(", decayFunction:");
    sb.append("fieldName: ").append(fieldName);
    sb.append("decayType: ").append(decayType);
    sb.append("origin: ").append(origin);
    sb.append("scale: ").append(scale);
    sb.append("offset: ").append(offset);
    sb.append("decay: ").append(decay);
    return sb.toString();
  }

  @Override
  protected FilterFunction doRewrite(
      IndexReader reader, boolean filterQueryRewritten, Query rewrittenFilterQuery) {
    if (filterQueryRewritten) {
      return new GeoPointDecayFilterFunction(
          rewrittenFilterQuery, getWeight(), decayFunction, indexState);
    } else {
      return this;
    }
  }

  @Override
  protected boolean doEquals(FilterFunction other) {
    if (other == null) {
      return false;
    }
    if (other.getClass() != this.getClass()) {
      return false;
    }
    GeoPointDecayFilterFunction otherGeoPointDecayFilterFunction =
        (GeoPointDecayFilterFunction) other;
    return Objects.equals(fieldName, otherGeoPointDecayFilterFunction.fieldName)
        && Objects.equals(decayType, otherGeoPointDecayFilterFunction.decayType)
        && Objects.equals(origin, otherGeoPointDecayFilterFunction.origin)
        && Double.compare(scale, otherGeoPointDecayFilterFunction.scale) == 0
        && Double.compare(offset, otherGeoPointDecayFilterFunction.offset) == 0
        && Double.compare(decay, otherGeoPointDecayFilterFunction.decay) == 0;
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(fieldName, decayType, origin, scale, offset, decay);
  }
}
