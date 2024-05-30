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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link org.apache.flink.core.state.StateFutureImpl.CallbackRunner} that put one mail in {@link
 * MailboxExecutor} but run multiple callbacks within one mail.
 */
public class BatchCallbackRunner {

    private static final int DEFAULT_BATCH_SIZE = 100;

    private final MailboxExecutor mailboxExecutor;

    private final int batchSize;

    private final ConcurrentLinkedDeque<ThrowingRunnable<? extends Exception>> callbackQueue;

    private final AtomicInteger currentMails = new AtomicInteger(0);

    private volatile boolean hasMail = false;

    private final Runnable newMailNotify;

    BatchCallbackRunner(MailboxExecutor mailboxExecutor, Runnable newMailNotify) {
        this.mailboxExecutor = mailboxExecutor;
        this.newMailNotify = newMailNotify;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.callbackQueue = new ConcurrentLinkedDeque<>();
    }

    public void submit(ThrowingRunnable<? extends Exception> task) {
        callbackQueue.offerLast(task);
        currentMails.getAndIncrement();
        insertMail(false);
    }

    public void insertMail(boolean force) {
        if (force || !hasMail) {
            synchronized (this) {
                if (force || !hasMail) {
                    if (currentMails.get() > 0) {
                        hasMail = true;
                        mailboxExecutor.execute(
                                this::runBatch, "Batch running callback of state requests");
                        notifyNewMail();
                    } else {
                        hasMail = false;
                    }
                }
            }
        }
    }

    public void runBatch() throws Exception {
        int count = 0;
        while (count < batchSize && currentMails.get() > 0) {
            ThrowingRunnable<? extends Exception> task = callbackQueue.pollFirst();
            if (task != null) {
                task.run();
                currentMails.decrementAndGet();
                count++;
            } else {
                break;
            }
        }
        insertMail(true);
    }

    private void notifyNewMail() {
        if (newMailNotify != null) {
            newMailNotify.run();
        }
    }

    public boolean isHasMail() {
        return hasMail;
    }
}
