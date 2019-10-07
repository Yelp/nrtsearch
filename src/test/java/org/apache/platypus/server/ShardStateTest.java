/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.platypus.server;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ShardStateTest {

    @Test
    public void isStarted() {
        Queue<String> queue = new ArrayBlockingQueue<String>(10);
        queue.add("1");
        queue.add("2");
        queue.add("3");

        List<String> foo = queue.stream().collect(Collectors.toList());
        queue.clear();
        Assert.assertEquals(queue.size(), 0);
        Assert.assertEquals(foo.size(), 3);
        foo.clear();
        Assert.assertEquals(foo.size(), 0);
        Assert.assertEquals(queue.size(), 0);

        long t0 = System.nanoTime();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long t1 = System.nanoTime();
        System.out.println((t1-t0)/1000000);
    }
}