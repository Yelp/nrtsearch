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
package com.yelp.nrtsearch.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that contains the compile time NRT Search version. The version string is loaded from a
 * properties file injected as part of the gradle build. Any failure to load the version results in
 * it being set to 'unknown'.
 */
public class Version {
  private static final Logger logger = LoggerFactory.getLogger(Version.class.getName());
  private static final String VERSION_PROPERTIES_FILE = "/version.properties";

  public static final String UNKNOWN_VERSION = "unknown";
  public static final Version CURRENT = new Version();

  private final String versionStr;

  private Version() {
    versionStr = getVersionFromProperties();
  }

  private String getVersionFromProperties() {
    InputStream propertiesStream = Version.class.getResourceAsStream(VERSION_PROPERTIES_FILE);
    if (propertiesStream == null) {
      logger.warn("Unable to find versions property resource: " + VERSION_PROPERTIES_FILE);
      return UNKNOWN_VERSION;
    }
    Properties properties = new Properties();
    try {
      properties.load(propertiesStream);
    } catch (IOException e) {
      logger.warn("Unable to load version properties file");
      return UNKNOWN_VERSION;
    }
    Object o = properties.get("version");
    if (o == null) {
      logger.warn("Version properties file does not contain version entry");
      return UNKNOWN_VERSION;
    }
    return o.toString();
  }

  /** Get the version string used for compilation. */
  @Override
  public String toString() {
    return versionStr;
  }
}
