/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.redis;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisBenchmarkConsumer implements BenchmarkConsumer {
    private final GenericObjectPool<StatefulRedisPubSubConnection<String, byte[]>> pool;
    private final String topic;
    private final String subscriptionName;
    private final String consumerId;
    private final ConsumerCallback consumerCallback;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private Future<?> consumerTask;
    private volatile boolean closing = false;

    public RedisBenchmarkConsumer(
            final String consumerId,
            final String topic,
            final String subscriptionName,
            final GenericObjectPool<StatefulRedisPubSubConnection<String, byte[]>> pool,
            ConsumerCallback consumerCallback) {
        this.pool = pool;
        this.topic = topic;
        this.subscriptionName = subscriptionName;
        this.consumerId = consumerId;
        this.consumerCallback = consumerCallback;
    }

    public void start() throws RejectedExecutionException {
        this.consumerTask =
                this.executor.submit(
                        () -> {
                            while (!this.closing) {
                                try (StatefulRedisPubSubConnection<String, byte[]> conn = this.pool.borrowObject()) {
                                    RedisCommands<String, byte[]> commands = conn.sync();

                                    List<StreamMessage<String, byte[]>> range =
                                            commands.xreadgroup(
                                                    Consumer.from(this.subscriptionName, this.consumerId),
                                                    XReadArgs.StreamOffset.lastConsumed(this.topic));
                                    for (StreamMessage<String, byte[]> streamEntry : range) {
                                        commands.xack(this.topic, this.subscriptionName, streamEntry.getId());
                                        long timestamp = Long.parseLong(streamEntry.getId().split("-")[0]);
                                        byte[] payload = streamEntry.getBody().get("payload");
                                        consumerCallback.messageReceived(payload, timestamp);
                                    }
                                } catch (InterruptedException | RedisCommandInterruptedException e) {
                                    // ignore
                                } catch (Exception e) {
                                    log.error("Failed to read from consumer instance.", e);
                                }
                            }
                        });
    }

    @Override
    public void close() throws Exception {
        closing = true;
        executor.shutdown();
        consumerTask.get();
        //        pool.close();
    }

    private static final Logger log = LoggerFactory.getLogger(RedisBenchmarkDriver.class);
}
