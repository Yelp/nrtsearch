/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.yelp.nrtsearch.server.utils;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Cache to hold the last n elements accessed, optimized for readers. The class is thread safe
 * through use of a {@link ReentrantReadWriteLock}. Interface functions that preform read operation only
 * use the read lock. Functions that modify cache membership (add/remove/clear/etc) use the write lock.
 * Read operations are O(1) and write operations are O(n) if an eviction is required.
 *
 * Each cache entry contains the last time it was accessed. This will be either the insertion time, or
 * the time of the last get operation. The timestamp is declared volatile to ensure atomic writes and
 * visibility.
 * @param <K> type of cache key
 * @param <V> type of cache value
 */
public class LRUCache<K, V> implements Map<K, V> {
    private static class CacheEntry<T> {
        T value;
        volatile long lastAccessTimeMs;

        CacheEntry(T value) {
            this.value = value;
            lastAccessTimeMs = System.currentTimeMillis();
        }
    }

    private final Map<K, CacheEntry<V>> cache = new HashMap<>();
    private final Integer maxSize;
    final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public LRUCache(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public int size() {
        rwLock.readLock().lock();
        try {
            return cache.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        rwLock.readLock().lock();
        try {
            return cache.isEmpty();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        rwLock.readLock().lock();
        try {
            return cache.containsKey(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        rwLock.readLock().lock();
        try {
            for (Map.Entry<K, CacheEntry<V>> entry : cache.entrySet()) {
                if (entry.getValue().value.equals(value)) {
                    return true;
                }
            }
            return false;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Get the value for a key. If the key exists, the last access time is updated to the
     * current time.
     * @param key cache key
     * @return value stored for the key, or null if not present
     */
    @Override
    public V get(Object key) {
        rwLock.readLock().lock();
        try {
            CacheEntry<V> entry = cache.get(key);
            if (entry != null) {
                entry.lastAccessTimeMs = System.currentTimeMillis();
                return entry.value;
            }
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Put value in the cache. If this insert pushed the size over maxSize,
     * the element with the oldest access time is removed.
     * @param key cache key
     * @param value value to insert
     * @return previous value for key, or null if none
     */
    @Override
    public V put(K key, V value) {
        rwLock.writeLock().lock();
        try {
            CacheEntry<V> lastValue = putInternal(key, value);
            return lastValue == null ? null : lastValue.value;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public V remove(Object key) {
        rwLock.writeLock().lock();
        try {
            CacheEntry<V> lastValue = cache.remove(key);
            return lastValue == null ? null : lastValue.value;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        rwLock.writeLock().lock();
        try {
            // TODO add efficient multi insert
            for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
                putInternal(entry.getKey(), entry.getValue());
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        rwLock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Set<K> keySet() {
        rwLock.readLock().lock();
        try {
            return cache.keySet();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Collection<V> values() {
        rwLock.readLock().lock();
        try {
            return cache.values().stream().map(entry -> entry.value).collect(Collectors.toList());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        rwLock.readLock().lock();
        try {
            return cache.entrySet()
                    .stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().value))
                    .collect(Collectors.toSet());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private CacheEntry<V> putInternal(K key, V value) {
        CacheEntry<V> entry = new CacheEntry<>(value);
        CacheEntry<V> previous = cache.put(key, entry);
        if (cache.size() > maxSize) {
            evictOldestAccess();
        }
        return previous;
    }

    private void evictOldestAccess() {
        if (cache.isEmpty()) {
            return;
        }
        long oldestAccess = Long.MAX_VALUE;
        K oldestKey = null;
        for(Map.Entry<K, CacheEntry<V>> entry : cache.entrySet()) {
            if (entry.getValue().lastAccessTimeMs < oldestAccess) {
                oldestAccess = entry.getValue().lastAccessTimeMs;
                oldestKey = entry.getKey();
            }
        }
        if (oldestKey != null) {
            cache.remove(oldestKey);
        }
    }
}
