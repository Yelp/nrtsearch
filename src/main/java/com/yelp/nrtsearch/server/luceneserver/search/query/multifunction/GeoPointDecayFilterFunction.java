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
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
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
  private final DecayFunction decayFunc;
  private final double scale;
  private final double offset;
  private final double decay;
  private final LatLng origin;

  /**
   * Constructor.
   *
   * @param filterQuery filter to use when applying this function, or null if none
   * @param weight weight multiple to scale the function score
   * @param decayFunction to score a document with a function that decays depending on the distance
   *     between an origin point and a numeric doc field value
   */
  public GeoPointDecayFilterFunction(
      Query filterQuery, float weight, MultiFunctionScoreQuery.DecayFunction decayFunction) {
    super(filterQuery, weight, decayFunction);
    this.decayFunction = decayFunction;
    this.fieldName = decayFunction.getFieldName();
    this.decayFunc = getDecayFunction(decayFunction.getDecayType());
    this.offset = GeoUtils.getDistance(decayFunction.getOffset());
    this.decay = decayFunction.getDecay();
    double userGivenScale = GeoUtils.getDistance(decayFunction.getScale());
    this.scale = decayFunc.computeScale(userGivenScale, decay);
    if (!decayFunction.hasGeoPoint()) {
      throw new IllegalArgumentException("Decay Function should have a geoPoint for Origin field");
    } else {
      this.origin = decayFunction.getGeoPoint();
    }
  }

  @Override
  public LeafFunction getLeafFunction(LeafReaderContext leafContext) throws IOException {
    return new GeoPointDecayLeafFunction(leafContext);
  }

  public final class GeoPointDecayLeafFunction implements LeafFunction {

    private final LoadedDocValues.SingleLocation geoPointValue;

    public GeoPointDecayLeafFunction(LeafReaderContext context) throws IOException {
      this.geoPointValue =
          new LoadedDocValues.SingleLocation(context.reader().getSortedNumericDocValues(fieldName));
    }

    @Override
    public double score(int docId, float innerQueryScore) throws IOException {
      GeoPoint geoPoint = getGeoPoint(docId);
      double distance =
          GeoUtils.arcDistance(
              origin.getLatitude(), origin.getLongitude(), geoPoint.getLat(), geoPoint.getLon());
      double score = decayFunc.computeScore(distance, scale);
      return Math.max(0.0, score - offset);
    }

    public GeoPoint getGeoPoint(int docId) throws IOException {
      geoPointValue.setDocId(docId);
      return geoPointValue.getValue();
    }

    @Override
    public Explanation explainScore(int docId, Explanation innerQueryScore) throws IOException {
      GeoPoint geoPoint = getGeoPoint(docId);
      double distance =
          GeoUtils.arcDistance(
              origin.getLatitude(), origin.getLongitude(), geoPoint.getLat(), geoPoint.getLon());
      double score = Math.max(0.0, decayFunc.computeScore(distance, scale) - offset);
      Explanation paramsExp =
          Explanation.match(distance, "arc distance calculated between two geoPoints");
      return Explanation.match(
          score,
          "final score with the provided decay function calculated by max(0.0, score - offset) with "
              + offset
              + " offset value",
          List.of(
              paramsExp, decayFunc.explainComputeScore(String.valueOf(distance), distance, scale)));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(", decayFunction:");
    sb.append("fieldName: ").append(fieldName);
    sb.append("decayFunc: ").append(decayFunc);
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
      return new GeoPointDecayFilterFunction(rewrittenFilterQuery, getWeight(), decayFunction);
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
    return Objects.equals(origin, otherGeoPointDecayFilterFunction.origin)
        && decayFunc.equals(otherGeoPointDecayFilterFunction.decayFunc)
        && Objects.equals(fieldName, otherGeoPointDecayFilterFunction.fieldName)
        && Double.compare(scale, otherGeoPointDecayFilterFunction.scale) == 0
        && Double.compare(offset, otherGeoPointDecayFilterFunction.offset) == 0
        && Double.compare(decay, otherGeoPointDecayFilterFunction.decay) == 0;
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(fieldName, decayFunc, scale, offset, decay, origin);
  }
}
