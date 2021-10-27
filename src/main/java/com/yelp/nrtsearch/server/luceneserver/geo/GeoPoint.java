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
package com.yelp.nrtsearch.server.luceneserver.geo;

import it.unimi.dsi.fastutil.doubles.DoubleDoublePair;
import org.apache.lucene.util.SloppyMath;

/**
 * Class to encapsulate a geo location loaded out of a doc value. Currently represents a lat/lon
 * value. Also provides point with a {@link DoubleDoublePair} view, with left/first being latitude
 * and right/second being longitude.
 */
public final class GeoPoint implements DoubleDoublePair {
  private final double latitude;
  private final double longitude;

  public GeoPoint(double latitude, double longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public double getLat() {
    return latitude;
  }

  public double getLon() {
    return longitude;
  }

  public double arcDistance(double lat, double lon) {
    return SloppyMath.haversinMeters(latitude, longitude, lat, lon);
  }

  @Override
  public int hashCode() {
    return 31 * Double.hashCode(latitude) + Double.hashCode(longitude);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof GeoPoint) {
      GeoPoint point = (GeoPoint) o;
      return Double.compare(point.latitude, latitude) == 0
          && Double.compare(point.longitude, longitude) == 0;
    }
    return false;
  }

  @Override
  public String toString() {
    return new StringBuilder("GeoPoint(")
        .append("latitude=")
        .append(latitude)
        .append(", longitude=")
        .append(longitude)
        .append(")")
        .toString();
  }

  @Override
  public double leftDouble() {
    return latitude;
  }

  @Override
  public double rightDouble() {
    return longitude;
  }
}
