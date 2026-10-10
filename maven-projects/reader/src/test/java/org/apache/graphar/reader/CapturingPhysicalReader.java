/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;

/** Records every request before passing it to the wrapped reader. */
final class CapturingPhysicalReader implements PhysicalReader {
    final List<ReadRequest> requests = new ArrayList<>();
    private final PhysicalReader delegate;

    CapturingPhysicalReader(PhysicalReader delegate) {
        this.delegate = delegate;
    }

    @Override
    public Set<ReadCapability> capabilities() {
        return delegate.capabilities();
    }

    @Override
    public ReadResult read(ReadRequest request) throws IOException {
        requests.add(request);
        return delegate.read(request);
    }
}
