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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LRUCacheTest {

    private void initSimple(LRUCache<Integer, Integer> cache) {
        cache.put(1, 2);
        cache.put(3, 4);
        cache.put(5, 6);
        cache.put(7, 8);
        cache.put(9, 10);
    }

    private void initSimpleWithDelay(LRUCache<Integer, Integer> cache) throws Exception {
        cache.put(1, 2);
        Thread.sleep(10);
        cache.put(3, 4);
        Thread.sleep(10);
        cache.put(5, 6);
        Thread.sleep(10);
        cache.put(7, 8);
        Thread.sleep(10);
        cache.put(9, 10);
        Thread.sleep(10);
    }

    @Test
    public void testContainsKey() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());
        assertTrue(cache.containsKey(1));
        assertTrue(cache.containsKey(3));
        assertTrue(cache.containsKey(5));
        assertTrue(cache.containsKey(7));
        assertTrue(cache.containsKey(9));
        assertFalse(cache.containsKey(2));
    }


    @Test
    public void testContainsValue() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());
        assertTrue(cache.containsValue(2));
        assertTrue(cache.containsValue(4));
        assertTrue(cache.containsValue(6));
        assertTrue(cache.containsValue(8));
        assertTrue(cache.containsValue(10));
        assertFalse(cache.containsValue(3));
    }

    @Test
    public void testRemove() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());
        Integer previous = cache.remove(5);
        assertEquals(4, cache.size());
        assertFalse(cache.containsKey(5));
        assertFalse(cache.containsValue(6));
        assertEquals(Integer.valueOf(6), previous);

        previous = cache.remove(11);
        assertEquals(4, cache.size());
        assertNull(previous);
    }

    @Test
    public void testClear() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void testKeySet() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(new HashSet<>(Arrays.asList(1, 3, 5, 7, 9)), cache.keySet());
    }

    @Test
    public void testValueSet() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(Arrays.asList(2, 4, 6, 8, 10), cache.values());
    }

    @Test
    public void testFixedSizeWithPut() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());
        cache.put(11, 12);
        cache.put(13, 14);
        cache.put(15, 16);
        cache.put(17, 18);
        cache.put(19, 20);
        assertEquals(5, cache.size());
    }

    @Test
    public void testFixedSizeWithPutAll() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());
        Map<Integer, Integer> putMap = new HashMap<>();
        putMap.put(11, 12);
        putMap.put(13, 14);
        putMap.put(15, 16);
        cache.putAll(putMap);
        assertEquals(5, cache.size());

        putMap = new HashMap<>();
        putMap.put(17, 18);
        putMap.put(19, 20);
        putMap.put(21, 22);
        putMap.put(23, 24);
        putMap.put(25, 26);
        putMap.put(27, 28);
        putMap.put(29, 30);
        cache.putAll(putMap);
        assertEquals(5, cache.size());
    }

    @Test
    public void testEvictOldest() throws Exception {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimpleWithDelay(cache);
        assertEquals(5, cache.size());

        cache.put(11, 12);
        Thread.sleep(10);
        assertTrue(cache.containsKey(11));
        assertTrue(cache.containsValue(12));
        assertFalse(cache.containsKey(1));
        assertFalse(cache.containsValue(2));

        cache.put(13, 14);
        Thread.sleep(10);
        assertTrue(cache.containsKey(13));
        assertTrue(cache.containsValue(14));
        assertFalse(cache.containsKey(3));
        assertFalse(cache.containsValue(4));

        cache.put(15, 16);
        Thread.sleep(10);
        assertTrue(cache.containsKey(15));
        assertTrue(cache.containsValue(16));
        assertFalse(cache.containsKey(5));
        assertFalse(cache.containsValue(6));

        cache.put(17, 18);
        Thread.sleep(10);
        assertTrue(cache.containsKey(17));
        assertTrue(cache.containsValue(18));
        assertFalse(cache.containsKey(7));
        assertFalse(cache.containsValue(8));
    }

    @Test
    public void testGetUpdatesAccessTime() throws Exception {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimpleWithDelay(cache);
        assertEquals(5, cache.size());

        Integer value = cache.get(1);
        Thread.sleep(10);
        assertEquals(Integer.valueOf(2), value);

        checkAccessTimeUpdate(cache, 2);
    }

    @Test
    public void testPutUpdatesAccessTime() throws Exception {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimpleWithDelay(cache);
        assertEquals(5, cache.size());

        cache.put(1, 100);
        Thread.sleep(10);

        checkAccessTimeUpdate(cache, 100);
    }

    @Test
    public void testPutAllUpdatesAccessTime() throws Exception {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimpleWithDelay(cache);
        assertEquals(5, cache.size());

        Map<Integer, Integer> putMap = new HashMap<>();
        putMap.put(1, 100);
        cache.putAll(putMap);
        Thread.sleep(10);

        checkAccessTimeUpdate(cache, 100);
    }

    private void checkAccessTimeUpdate(LRUCache<Integer, Integer> cache, Integer value) throws Exception {
        cache.put(11, 12);
        Thread.sleep(10);
        assertTrue(cache.containsKey(11));
        assertTrue(cache.containsValue(12));
        assertTrue(cache.containsKey(1));
        assertTrue(cache.containsValue(value));
        assertFalse(cache.containsKey(3));
        assertFalse(cache.containsValue(4));

        cache.put(13, 14);
        Thread.sleep(10);
        assertTrue(cache.containsKey(13));
        assertTrue(cache.containsValue(14));
        assertTrue(cache.containsKey(1));
        assertTrue(cache.containsValue(value));
        assertFalse(cache.containsKey(5));
        assertFalse(cache.containsValue(6));

        cache.put(15, 16);
        Thread.sleep(10);
        assertTrue(cache.containsKey(15));
        assertTrue(cache.containsValue(16));
        assertTrue(cache.containsKey(1));
        assertTrue(cache.containsValue(value));
        assertFalse(cache.containsKey(7));
        assertFalse(cache.containsValue(8));

        cache.put(17, 18);
        Thread.sleep(10);
        assertTrue(cache.containsKey(17));
        assertTrue(cache.containsValue(18));
        assertTrue(cache.containsKey(1));
        assertTrue(cache.containsValue(value));
        assertFalse(cache.containsKey(9));
        assertFalse(cache.containsValue(10));

        cache.put(19, 20);
        Thread.sleep(10);
        assertTrue(cache.containsKey(19));
        assertTrue(cache.containsValue(20));
        assertFalse(cache.containsKey(1));
        assertFalse(cache.containsValue(value));
    }

    @Test
    public void testWriteLock() throws Exception {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());

        cache.rwLock.writeLock().lock();

        Thread getThread = new Thread(() -> {
            Integer value = cache.get(1);
            assertEquals(Integer.valueOf(2), value);
        });

        Thread putThread = new Thread(() -> {
            cache.put(11, 12);
        });

        getThread.start();
        putThread.start();
        Thread.sleep(1000);
        assertTrue(getThread.isAlive());
        assertTrue(putThread.isAlive());

        cache.rwLock.writeLock().unlock();
        long waitInterval = 100;
        long waitTime = 0;
        while (getThread.isAlive()) {
            Thread.sleep(waitInterval);
            waitTime += waitInterval;
            if (waitTime > 5000) {
                fail("Get thread did not complete");
            }
        }
        while (putThread.isAlive()) {
            Thread.sleep(waitInterval);
            waitTime += waitInterval;
            if (waitTime > 5000) {
                fail("Put thread did not complete");
            }
        }
    }

    @Test
    public void testReadLock() throws Exception {
        LRUCache<Integer, Integer> cache = new LRUCache<>(5);
        initSimple(cache);
        assertEquals(5, cache.size());

        cache.rwLock.readLock().lock();

        Thread getThread = new Thread(() -> {
            Integer value = cache.get(1);
            assertEquals(Integer.valueOf(2), value);
        });

        Thread putThread = new Thread(() -> {
            cache.put(11, 12);
        });

        getThread.start();
        putThread.start();
        Thread.sleep(1000);
        assertFalse(getThread.isAlive());
        assertTrue(putThread.isAlive());

        cache.rwLock.readLock().unlock();
        long waitInterval = 100;
        long waitTime = 0;
        while (putThread.isAlive()) {
            Thread.sleep(waitInterval);
            waitTime += waitInterval;
            if (waitTime > 5000) {
                fail("Put thread did not complete");
            }
        }
    }
}
