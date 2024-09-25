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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

/** A factory to open a {@link Directory} from a provided filesystem path. */
public abstract class DirectoryFactory {

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
  public static DirectoryFactory get(final String dirImpl) {
    if (dirImpl.equals("FSDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException {
          Directory directory = FSDirectory.open(path);
          if (directory instanceof MMapDirectory mMapDirectory) {
            mMapDirectory.setPreload(preloadConfig.preloadPredicate());
          }
          return directory;
        }
      };
    } else if (dirImpl.equals("MMapDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException {
          MMapDirectory mMapDirectory = new MMapDirectory(path);
          mMapDirectory.setPreload(preloadConfig.preloadPredicate());
          return mMapDirectory;
        }
      };
    } else if (dirImpl.equals("NIOFSDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path, IndexPreloadConfig preloadConfig) throws IOException {
          return new NIOFSDirectory(path);
        }
      };
    } else {
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
          } catch (InstantiationException ie) {
            throw new RuntimeException(
                "failed to instantiate directory class \""
                    + dirImpl
                    + "\" on path=\""
                    + path
                    + "\"",
                ie);
          } catch (InvocationTargetException ite) {
            throw new RuntimeException(
                "failed to instantiate directory class \""
                    + dirImpl
                    + "\" on path=\""
                    + path
                    + "\"",
                ite);
          } catch (IllegalAccessException iae) {
            throw new RuntimeException(
                "failed to instantiate directory class \""
                    + dirImpl
                    + "\" on path=\""
                    + path
                    + "\"",
                iae);
          }
        }
      };
    }
  }
}
