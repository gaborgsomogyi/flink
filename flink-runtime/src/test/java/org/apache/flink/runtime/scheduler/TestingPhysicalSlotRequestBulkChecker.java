/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulk;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;

import java.time.Duration;

/** A {@link PhysicalSlotRequestBulkChecker} implementation that can be used in tests. */
public class TestingPhysicalSlotRequestBulkChecker implements PhysicalSlotRequestBulkChecker {

    private PhysicalSlotRequestBulk bulk;
    private Duration timeout;

    @Override
    public void start(ComponentMainThreadExecutor mainThreadExecutor) {}

    @Override
    public void schedulePendingRequestBulkTimeoutCheck(
            PhysicalSlotRequestBulk bulk, Duration timeout) {
        this.bulk = bulk;
        this.timeout = timeout;
    }

    PhysicalSlotRequestBulk getBulk() {
        return bulk;
    }

    Duration getTimeout() {
        return timeout;
    }
}
