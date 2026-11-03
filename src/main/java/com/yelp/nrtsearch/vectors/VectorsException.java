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
package com.yelp.nrtsearch.vectors;

/** Error raised by the vector gateway, modeled after S3 Vectors error codes. */
public class VectorsException extends RuntimeException {
  private final String code;
  private final int httpStatus;

  public VectorsException(String code, int httpStatus, String message) {
    super(message);
    this.code = code;
    this.httpStatus = httpStatus;
  }

  public static VectorsException validation(String message) {
    return new VectorsException("ValidationException", 400, message);
  }

  public static VectorsException notFound(String message) {
    return new VectorsException("NotFoundException", 404, message);
  }

  public static VectorsException conflict(String message) {
    return new VectorsException("ConflictException", 409, message);
  }

  public String getCode() {
    return code;
  }

  public int getHttpStatus() {
    return httpStatus;
  }
}
