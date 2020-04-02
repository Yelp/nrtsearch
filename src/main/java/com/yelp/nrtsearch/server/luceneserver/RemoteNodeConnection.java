/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.yelp.nrtsearch.server.luceneserver;

import java.io.Closeable;
import java.io.IOException;

/**
 * Persistent connections to other nodes in the cluster for us to send commands to them
 */
public final class RemoteNodeConnection implements Closeable {
    public final byte[] nodeID;
    public final Connection c;

    public RemoteNodeConnection(byte[] nodeID, Connection c) {
        this.nodeID = nodeID;
        this.c = c;
    }

    @Override
    public void close() throws IOException {
        c.close();
    }
}
