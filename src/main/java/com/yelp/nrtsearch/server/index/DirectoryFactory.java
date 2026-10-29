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
package com.yelp.nrtsearch.server.index;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

/** A factory to open a {@link Directory} from a provided filesystem path. */
public abstract class DirectoryFactory {

  // How files should be grouped in memory Arenas when using MMapDirectory
  public enum MMapGrouping {
    SEGMENT,
    SEGMENT_EXCEPT_SI,
    NONE
  }

  /** How the read advice for memory mapped files is chosen when using MMapDirectory. */
  public enum MMapReadAdvice {
    /** Apply no read advice, using the OS default. This is the Lucene default. */
    NONE,
    /**
     * Apply the read advice requested by the codec in the IOContext, such as random access for
     * vector data. See {@link MMapDirectory#ADVISE_BY_CONTEXT}.
     */
    BY_CONTEXT
  }

  /** Sole constructor. */
  public DirectoryFactory() {}

  /**
   * Open a new {@link Directory} at the specified path.
   *
   * @param path directory path
   * @param preloadConfig config for preloading index data into memory (only for MMapDirectory type)
   */
  public abstract Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException;

  /**
   * Returns an instance, using the specified implementation {FSDirectory, MMapDirectory,
   * NIOFSDirectory}.
   */
  public static DirectoryFactory get(final String dirImpl, NrtsearchConfig config) {
    switch (dirImpl) {
      case "FSDirectory" -> {
        return new DirectoryFactory() {
          @Override
          public Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException {
            Directory directory = FSDirectory.open(path);
            if (directory instanceof MMapDirectory mMapDirectory) {
              mMapDirectory.setPreload(preloadConfig.preloadPredicate());
              setMMapGrouping(mMapDirectory, config.getMMapGrouping());
              setMMapReadAdvice(mMapDirectory, config.getMMapReadAdvice());
            }
            return directory;
          }
        };
      }
      case "MMapDirectory" -> {
        return new DirectoryFactory() {
          @Override
          public Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException {
            MMapDirectory mMapDirectory = new MMapDirectory(path);
            mMapDirectory.setPreload(preloadConfig.preloadPredicate());
            setMMapGrouping(mMapDirectory, config.getMMapGrouping());
            setMMapReadAdvice(mMapDirectory, config.getMMapReadAdvice());
            return mMapDirectory;
          }
        };
      }
      case "NIOFSDirectory" -> {
        return new DirectoryFactory() {
          @Override
          public Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException {
            return new NIOFSDirectory(path);
          }
        };
      }
      default -> {
        final Class<? extends Directory> dirClass;
        try {
          dirClass = Class.forName(dirImpl).asSubclass(Directory.class);
        } catch (ClassNotFoundException cnfe) {
          throw new IllegalArgumentException(
              "could not locate Directory sub-class \"" + dirImpl + "\"; verify CLASSPATH");
        }
        Constructor<? extends Directory> ctor = null;
        try {
          ctor = dirClass.getConstructor(Path.class);
        } catch (NoSuchMethodException ignored) {
        }
        try {
          ctor = dirClass.getConstructor(Path.class, IndexPreloadConfig.class);
        } catch (NoSuchMethodException ignored) {
        }
        if (ctor == null) {
          throw new IllegalArgumentException(
              "class \""
                  + dirImpl
                  + "\" does not have a constructor taking a single Path argument, or a Path and a boolean");
        }

        final Constructor<? extends Directory> finalCtor = ctor;
        return new DirectoryFactory() {
          @Override
          public Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException {
            try {
              if (finalCtor.getParameterCount() == 1) {
                return finalCtor.newInstance(path);
              } else {
                return finalCtor.newInstance(path, preloadConfig);
              }
            } catch (InstantiationException
                | InvocationTargetException
                | IllegalAccessException ie) {
              throw new RuntimeException(
                  "failed to instantiate directory class \""
                      + dirImpl
                      + "\" on path=\""
                      + path
                      + "\"",
                  ie);
            }
          }
        };
      }
    }
  }

  // Function to group segments by their names, excluding ".si" files
  public static Function<String, Optional<String>> SEGMENT_EXCEPT_SI_FUNCTION =
      (filename) -> {
        if (filename.endsWith(".si")) {
          return Optional.empty();
        }
        return MMapDirectory.GROUP_BY_SEGMENT.apply(filename);
      };

  /**
   * Set MMapGrouping for the directory.
   *
   * @param directory the MMapDirectory
   * @param grouping the MMapGrouping
   */
  @VisibleForTesting
  static void setMMapGrouping(MMapDirectory directory, MMapGrouping grouping) {
    switch (grouping) {
      case SEGMENT -> directory.setGroupingFunction(MMapDirectory.GROUP_BY_SEGMENT);
      case SEGMENT_EXCEPT_SI -> directory.setGroupingFunction(SEGMENT_EXCEPT_SI_FUNCTION);
      case NONE -> directory.setGroupingFunction(MMapDirectory.NO_GROUPING);
    }
  }

  /**
   * Set the read advice function for the directory.
   *
   * @param directory the MMapDirectory
   * @param readAdvice the MMapReadAdvice
   */
  @VisibleForTesting
  static void setMMapReadAdvice(MMapDirectory directory, MMapReadAdvice readAdvice) {
    switch (readAdvice) {
      case NONE -> {}
      case BY_CONTEXT -> directory.setReadAdvice(MMapDirectory.ADVISE_BY_CONTEXT);
    }
  }

  /**
   * Parse MMapReadAdvice from string.
   *
   * @param readAdvice the string representation of the read advice
   * @return MMapReadAdvice
   * @throws IllegalArgumentException if the read advice is invalid
   */
  public static MMapReadAdvice parseMMapReadAdvice(String readAdvice) {
    return switch (readAdvice) {
      case "NONE" -> MMapReadAdvice.NONE;
      case "BY_CONTEXT" -> MMapReadAdvice.BY_CONTEXT;
      default -> throw new IllegalArgumentException("Invalid MMapReadAdvice: " + readAdvice);
    };
  }

  /**
   * Parse MMapGrouping from string.
   *
   * @param grouping the string representation of the grouping
   * @return MMapGrouping
   * @throws IllegalArgumentException if the grouping is invalid
   */
  public static MMapGrouping parseMMapGrouping(String grouping) {
    return switch (grouping) {
      case "SEGMENT" -> MMapGrouping.SEGMENT;
      case "SEGMENT_EXCEPT_SI" -> MMapGrouping.SEGMENT_EXCEPT_SI;
      case "NONE" -> MMapGrouping.NONE;
      default -> throw new IllegalArgumentException("Invalid MMapGrouping: " + grouping);
    };
  }
}
