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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

// import org.apache.lucene.store.AsyncFSDirectory;

/** A factory to open a {@link Directory} from a provided filesystem path. */
public abstract class DirectoryFactory {

  /** Sole constructor. */
  public DirectoryFactory() {}

  /** Open a new {@link Directory} at the specified path. */
  public abstract Directory open(Path path) throws IOException;

  /**
   * Returns an instance, using the specified implementation {FSDirectory, MMapDirectory,
   * NIOFSDirectory, SimpleFSDirectory or RAMDirectory}.
   */
  public static DirectoryFactory get(final String dirImpl) {
    if (dirImpl.equals("FSDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path) throws IOException {
          return FSDirectory.open(path);
        }
      };
    } else if (dirImpl.equals("MMapDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path) throws IOException {
          MMapDirectory mMapDirectory = new MMapDirectory(path);
          mMapDirectory.setPreload(true);
          return mMapDirectory;
        }
      };
    } else if (dirImpl.equals("NIOFSDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path) throws IOException {
          return new NIOFSDirectory(path);
        }
      };
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path) throws IOException {
          return new SimpleFSDirectory(path);
        }
      };
    } else if (dirImpl.equals("RAMDirectory")) {
      return new DirectoryFactory() {
        @Override
        public Directory open(Path path) throws IOException {
          return new RAMDirectory();
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
      final Constructor<? extends Directory> ctor;
      try {
        ctor = dirClass.getConstructor(Path.class);
      } catch (NoSuchMethodException nsme) {
        throw new IllegalArgumentException(
            "class \"" + dirImpl + "\" does not have a constructor taking a single Path argument");
      }

      return new DirectoryFactory() {
        @Override
        public Directory open(Path path) throws IOException {
          try {
            return ctor.newInstance(path);
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
