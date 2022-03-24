/*
 * Copyright 2021 Yelp Inc.
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SaveState {

  /** Wrapper class around JSONObject which implements thread safety using read-write lock */
  public static class ThreadSafeJSONObject {

    /** JSON object for storing the data */
    final JsonObject data = new JsonObject();

    /** Locking mechanism to guarantee the thread safety */
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    public void add(String property, JsonElement value) {
      try {
        writeLock.lock();
        data.add(property, value);
      } finally {
        writeLock.unlock();
      }
    }

    public void addProperty(String property, String value) {
      try {
        writeLock.lock();
        data.addProperty(property, value);
      } finally {
        writeLock.unlock();
      }
    }

    public void addProperty(String property, Number value) {
      try {
        writeLock.lock();
        data.addProperty(property, value);
      } finally {
        writeLock.unlock();
      }
    }

    public void addProperty(String property, Boolean value) {
      try {
        writeLock.lock();
        data.addProperty(property, value);
      } finally {
        writeLock.unlock();
      }
    }

    public JsonElement get(String memberName) {
      try {
        readLock.lock();
        return data.get(memberName);
      } finally {
        readLock.unlock();
      }
    }

    public JsonElement remove(String property) {
      try {
        writeLock.lock();
        return data.remove(property);
      } finally {
        writeLock.unlock();
      }
    }

    public boolean has(String memberName) {
      try {
        readLock.lock();
        return data.has(memberName);
      } finally {
        readLock.unlock();
      }
    }

    public JsonObject getData() {
      try {
        readLock.lock();
        return data;
      } finally {
        readLock.unlock();
      }
    }

    @Override
    public String toString() {
      try {
        readLock.lock();
        return data.toString();
      } finally {
        readLock.unlock();
      }
    }
  }

  final ThreadSafeJSONObject settings = new ThreadSafeJSONObject();
  final ThreadSafeJSONObject liveSettings = new ThreadSafeJSONObject();
  final ThreadSafeJSONObject fields = new ThreadSafeJSONObject();
  final ThreadSafeJSONObject suggest = new ThreadSafeJSONObject();

  public ThreadSafeJSONObject getSettings() {
    return settings;
  }

  public ThreadSafeJSONObject getLiveSettings() {
    return liveSettings;
  }

  public ThreadSafeJSONObject getFields() {
    return fields;
  }

  public ThreadSafeJSONObject getSuggest() {
    return suggest;
  }

  public JsonObject getSaveState() throws IOException {
    JsonObject o = new JsonObject();
    o.add("settings", settings.getData());
    o.add("liveSettings", liveSettings.getData());
    o.add("fields", fields.getData());
    o.add("suggest", suggest.getData());
    return o;
  }
}
