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
package com.yelp.nrtsearch.server.config;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * General purpose Yaml config file reader that allows value lookup by providing dot separated key
 * paths. The path represents the traversal through yaml hashes. The input file is expected to
 * contain a single document that is a hash. For example:
 *
 * <pre>
 *     a1:
 *       a2:
 *         key1: val1
 *       a3:
 *         key2: val2
 *     a2:
 *       key3: val3
 *     ...
 * </pre>
 *
 * The key "a1.a3.key2" would have the value "val2".
 *
 * <p>Reading lists and getting the list of a hash's keys are also supported.
 *
 * <p>Some helper functions are include for the reading of basic types, and extension is possible by
 * using the generic versions of the lookup functions.
 *
 * <p>All lookup functions throw {@link NullPointerException} if the provided path or reader
 * function is null.
 *
 * <p>Lookup functions without default values will throw a checked {@link
 * ConfigKeyNotFoundException} if any component of the lookup path does not exist in the config.
 *
 * <p>Lookup functions can throw an unchecked {@link ConfigReadException} if the config is malformed
 * for the given key path. This will occur if a component of the key path is not a hash, such as
 * "key3" in path "a2.key3.a4" in the example above. This will also occur if the key value is not
 * parsable into the desired type.
 */
public class YamlConfigReader {
  private static final Logger logger = LoggerFactory.getLogger(YamlConfigReader.class.getName());

  private static Function<Object, String> STR_READER =
      (node) -> {
        if (!(node instanceof Collection) && !(node instanceof Map)) {
          return node.toString();
        }
        return null;
      };
  private static Function<Object, Boolean> BOOL_READER =
      (node) -> {
        if (node instanceof Boolean) {
          return (Boolean) node;
        }

        if (node instanceof String) {
          return Boolean.parseBoolean((String) node);
        }
        return null;
      };
  private static Function<Object, Integer> INT_READER =
      new NumberReaderFunc<>(Integer.class, Number::intValue, Integer::parseInt);
  private static Function<Object, Long> LONG_READER =
      new NumberReaderFunc<>(Long.class, Number::longValue, Long::parseLong);
  private static Function<Object, Float> FLOAT_READER =
      new NumberReaderFunc<>(Float.class, Number::floatValue, Float::parseFloat);
  private static Function<Object, Double> DOUBLE_READER =
      new NumberReaderFunc<>(Double.class, Number::doubleValue, Double::parseDouble);

  private static String SPLIT_REGEX = "[.]";

  private final Map<?, ?> configRoot;

  public YamlConfigReader(InputStream fileStream) {
    Yaml yaml = new Yaml();
    Object configObject = yaml.load(fileStream);
    if (!(configObject instanceof Map)) {
      throw new IllegalArgumentException("Loaded yaml file must be a hash.");
    }
    configRoot = (Map<?, ?>) configObject;
  }

  /**
   * Read String config value. Must not be array or hash.
   *
   * @param path dot separated key path
   * @return the .toString() value of config entry
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public String getString(String path) throws ConfigKeyNotFoundException {
    return get(path, STR_READER);
  }

  /**
   * Read String config value, or default if not found. Must not be array or hash.
   *
   * @param path dot separated key path
   * @param def default value
   * @return the .toString() value of config entry, or default if not found
   */
  public String getString(String path, String def) {
    return get(path, STR_READER, def);
  }

  /**
   * Read array of String values.
   *
   * @param path dot separated key path
   * @return list of .toString() values of items in specified array
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public List<String> getStringList(String path) throws ConfigKeyNotFoundException {
    return getList(path, STR_READER);
  }

  /**
   * Read array of String values, or default if not found.
   *
   * @param path dot separated key path
   * @param def default value
   * @return list of .toString() values of items in specified array, or default if not found
   */
  public List<String> getStringList(String path, List<String> def) {
    return getList(path, STR_READER, def);
  }

  /**
   * Read Boolean config value. Must be a boolean or a string parsable with Boolean::parseBoolean().
   *
   * @param path dot separated key path
   * @return Boolean value of config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public Boolean getBoolean(String path) throws ConfigKeyNotFoundException {
    return get(path, BOOL_READER);
  }

  /**
   * Read Boolean config value, of default if not found. Must be a boolean or a string parsable with
   * Boolean::parseBoolean().
   *
   * @param path dot separated key path
   * @param def default value
   * @return Boolean value of config key, of default if not found
   */
  public Boolean getBoolean(String path, Boolean def) {
    return get(path, BOOL_READER, def);
  }

  /**
   * Read array of Boolean config values. Items must be a boolean or a string parsable with
   * Boolean::parseBoolean().
   *
   * @param path dot separated key path
   * @return list of Boolean values for config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public List<Boolean> getBooleanList(String path) throws ConfigKeyNotFoundException {
    return getList(path, BOOL_READER);
  }

  /**
   * Read array of Boolean config values, or default if not found. Items must be a boolean or a
   * string parsable with Boolean::parseBoolean().
   *
   * @param path dot separated key path
   * @return list of Boolean values for config key, or default if not found
   */
  public List<Boolean> getBooleanList(String path, List<Boolean> def) {
    return getList(path, BOOL_READER, def);
  }

  /**
   * Read Integer config value. Must be a {@link Number} or a string parsable with
   * Integer::parseInteger().
   *
   * @param path dot separated key path
   * @return Integer value of config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public Integer getInteger(String path) throws ConfigKeyNotFoundException {
    return get(path, INT_READER);
  }

  /**
   * Read Integer config value, or default if not found. Must be a {@link Number} or a string
   * parsable with Integer::parseInteger().
   *
   * @param path dot separated key path
   * @param def default value
   * @return Integer value of config key, or default if not found
   */
  public Integer getInteger(String path, Integer def) {
    return get(path, INT_READER, def);
  }

  /**
   * Read array of Integer config values. Items must be a {@link Number} or a string parsable with
   * Integer::parseInteger().
   *
   * @param path dot separated key path
   * @return list of Integer values for config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public List<Integer> getIntegerList(String path) throws ConfigKeyNotFoundException {
    return getList(path, INT_READER);
  }

  /**
   * Read array of Integer config values, or default if not found. Items must be a {@link Number} or
   * a string parsable with Integer::parseInteger().
   *
   * @param path dot separated key path
   * @param def default value
   * @return list of Integer values for config key, or default if not found
   */
  public List<Integer> getIntegerList(String path, List<Integer> def) {
    return getList(path, INT_READER, def);
  }

  /**
   * Read Long config value. Must be a {@link Number} or a string parsable with Long::parseLong().
   *
   * @param path dot separated key path
   * @return Long value of config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public Long getLong(String path) throws ConfigKeyNotFoundException {
    return get(path, LONG_READER);
  }

  /**
   * Read Long config value, or default if not found. Must be a {@link Number} or a string parsable
   * with Long::parseLong().
   *
   * @param path dot separated key path
   * @param def default value
   * @return Long value of config key, or default if not found
   */
  public Long getLong(String path, Long def) {
    return get(path, LONG_READER, def);
  }

  /**
   * Read array of Long config values. Items must be a {@link Number} or a string parsable with
   * Long::parseLong().
   *
   * @param path dot separated key path
   * @return list of Long values for config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public List<Long> getLongList(String path) throws ConfigKeyNotFoundException {
    return getList(path, LONG_READER);
  }

  /**
   * Read array of Long config values, or default if not found. Items must be a {@link Number} or a
   * string parsable with Long::parseLong().
   *
   * @param path dot separated key path
   * @param def default value
   * @return list of Long values for config key, or default if not found
   */
  public List<Long> getLongList(String path, List<Long> def) {
    return getList(path, LONG_READER, def);
  }

  /**
   * Read Float config value. Must be a {@link Number} or a string parsable with
   * Float::parseFloat().
   *
   * @param path dot separated key path
   * @return Float value of config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public Float getFloat(String path) throws ConfigKeyNotFoundException {
    return get(path, FLOAT_READER);
  }

  /**
   * Read Float config value, or default if not found. Must be a {@link Number} or a string parsable
   * with Float::parseFloat().
   *
   * @param path dot separated key path
   * @param def default value
   * @return Float value of config key, or default if not found
   */
  public Float getFloat(String path, Float def) {
    return get(path, FLOAT_READER, def);
  }

  /**
   * Read array of Float config values. Items must be a {@link Number} or a string parsable with
   * Float::parseFloat().
   *
   * @param path dot separated key path
   * @return list of Float values for config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public List<Float> getFloatList(String path) throws ConfigKeyNotFoundException {
    return getList(path, FLOAT_READER);
  }

  /**
   * Read array of Float config values, or default if not found. Items must be a {@link Number} or a
   * string parsable with Float::parseFloat().
   *
   * @param path dot separated key path
   * @param def default value
   * @return list of Float values for config key, or default if not found
   */
  public List<Float> getFloatList(String path, List<Float> def) {
    return getList(path, FLOAT_READER, def);
  }

  /**
   * Read Double config value. Must be a {@link Number} or a string parsable with
   * Double::parseDouble().
   *
   * @param path dot separated key path
   * @return Double value of config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public Double getDouble(String path) throws ConfigKeyNotFoundException {
    return get(path, DOUBLE_READER);
  }

  /**
   * Read Double config value, or default if not found. Must be a {@link Number} or a string
   * parsable with Double::parseDouble().
   *
   * @param path dot separated key path
   * @param def default value
   * @return Double value of config key, or default if not found
   */
  public Double getDouble(String path, Double def) {
    return get(path, DOUBLE_READER, def);
  }

  /**
   * Read array of Double config values. Items must be a {@link Number} or a string parsable with
   * Double::parseDouble().
   *
   * @param path dot separated key path
   * @return list of Double values for config key
   * @throws ConfigKeyNotFoundException if any path component does not exist
   */
  public List<Double> getDoubleList(String path) throws ConfigKeyNotFoundException {
    return getList(path, DOUBLE_READER);
  }

  /**
   * Read array of Double config values, or default if not found. Items must be a {@link Number} or
   * a string parsable with Double::parseDouble().
   *
   * @param path dot separated key path
   * @param def default value
   * @return list of Double values for config key, or default if not found
   */
  public List<Double> getDoubleList(String path, List<Double> def) {
    return getList(path, DOUBLE_READER, def);
  }

  /**
   * Get a value from the config given a dot separated key path and providing a default value. If
   * any component of the key path does not exist, the default value is returned.
   *
   * @param path dot separated key path
   * @param readerFunc function to convert config Object into the desired type
   * @param def default value to return if any component of the key path is not found
   * @param <T> type to read config value as
   * @return value of the given key, or default if not found
   */
  public <T> T get(String path, Function<Object, T> readerFunc, T def) {
    try {
      return get(path, readerFunc);
    } catch (ConfigKeyNotFoundException e) {
      logger.debug(path + ": " + def + " (default)");
      return def;
    }
  }

  /**
   * Get a value from the config given a dot separated key path. The desired entry is located by
   * traversing the hashes in the path. The provided function converts the entry into the desired
   * type.
   *
   * @param path dot separated key path
   * @param readerFunc function to convert config Object into the desired type
   * @param <T> type to read config value as
   * @return value of the given config key
   * @throws ConfigKeyNotFoundException if any component of the key path does not exist
   */
  public <T> T get(String path, Function<Object, T> readerFunc) throws ConfigKeyNotFoundException {
    Objects.requireNonNull(path);
    Objects.requireNonNull(readerFunc);

    Object node = getObject(path);
    T readValue = readerFunc.apply(node);
    if (readValue == null) {
      throw new ConfigReadException("Unable to parse config entry: " + node + ", path: " + path);
    }
    logger.debug(path + ": " + readValue);
    return readValue;
  }

  /**
   * Get a list (yaml array) from the config given a dot separated key path and providing a default
   * value. If any component of the key path does not exist, the default value is returned.
   *
   * @param path dot separated key path
   * @param readerFunc function to convert config array entries into the desired type
   * @param def default value to return if any component of the key path is not found
   * @param <T> type to read array values as
   * @return list value for the given config array, or default if not found
   */
  public <T> List<T> getList(String path, Function<Object, T> readerFunc, List<T> def) {
    try {
      return getList(path, readerFunc);
    } catch (ConfigKeyNotFoundException e) {
      logger.debug(path + ": " + def + " (default)");
      return def;
    }
  }

  /**
   * Get a list (yaml array) from the config given a dot separated key path. The desired entry is
   * located by traversing the hashes in the path. The provided function converts the list entries
   * into the desired type.
   *
   * @param path dot separated key path
   * @param readerFunc function to convert config array entries into the desired type
   * @param <T> type to read array values as
   * @return list value for the given config array
   * @throws ConfigKeyNotFoundException if any component of the key path does not exist
   */
  public <T> List<T> getList(String path, Function<Object, T> readerFunc)
      throws ConfigKeyNotFoundException {
    Objects.requireNonNull(path);
    Objects.requireNonNull(readerFunc);

    Object node = getObject(path);
    if (!(node instanceof Iterable)) {
      throw new ConfigReadException("Not instance of array, path: " + path);
    }
    List<T> readList = new ArrayList<>();
    for (Object item : (Iterable<?>) node) {
      T readItem = readerFunc.apply(item);
      if (readItem == null) {
        throw new ConfigReadException("Unable to parse array entry: " + item + ", path: " + path);
      }
      readList.add(readItem);
    }
    logger.debug(path + ": " + readList);
    return readList;
  }

  /**
   * Get the keys present in the hash specified by the dot separated key path. These keys must be
   * Strings.
   *
   * @param path dot separated key path
   * @return list of keys present in the specified config hash
   * @throws ConfigKeyNotFoundException if any component of the key path does not exist
   */
  public List<String> getKeys(String path) throws ConfigKeyNotFoundException {
    Objects.requireNonNull(path);

    Object node = getObject(path);
    if (!(node instanceof Map)) {
      throw new ConfigReadException("Not instance of Map, path: " + path);
    }
    List<String> keys = new ArrayList<>();
    for (Object mapKey : ((Map) node).keySet()) {
      if (!(mapKey instanceof String)) {
        throw new ConfigReadException("Map contain non String key: " + mapKey + ", path: " + path);
      }
      keys.add((String) mapKey);
    }
    logger.debug(path + ": " + keys + " (keys)");
    return keys;
  }

  /**
   * Get the keys present in the hash specified by the dot separated key path, or an empty list if
   * any of the path components do not exist.
   *
   * @param path dot separated key path
   * @return list of keys present in the specified config hash, or empty list if not found
   */
  public List<String> getKeysOrEmpty(String path) {
    try {
      return getKeys(path);
    } catch (ConfigKeyNotFoundException e) {
      logger.debug(path + ": [] (keys, not found)");
      return Collections.emptyList();
    }
  }

  private List<String> getPathComponents(String path) {
    String[] splits = path.split(SPLIT_REGEX);
    List<String> componentList = Arrays.asList(splits);
    if (path.endsWith(".")) {
      componentList = new ArrayList<>(componentList);
      componentList.add("");
    }
    return componentList;
  }

  private Object getObject(String path) throws ConfigKeyNotFoundException {
    List<String> pathComponents = getPathComponents(path);
    Object currentNode = configRoot;
    for (String pathComponent : pathComponents) {
      if (!(currentNode instanceof Map)) {
        throw new ConfigReadException("Path components must be hashes for path: " + path);
      }
      currentNode = ((Map<?, ?>) currentNode).get(pathComponent);
      if (currentNode == null) {
        throw new ConfigKeyNotFoundException(pathComponent + " not found for path: " + path);
      }
    }
    return currentNode;
  }
}
