/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.remote.FileCompressor;
import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for providing custom {@link FileCompressor} implementations. Registered
 * compressors can be referenced by name in the {@code remoteConfig.s3.compressionType}
 * configuration key.
 */
public interface FileCompressorPlugin {

  /** Get map of compressor name to {@link FileCompressor} for registration. */
  default Map<String, FileCompressor> getFileCompressors() {
    return Collections.emptyMap();
  }
}
