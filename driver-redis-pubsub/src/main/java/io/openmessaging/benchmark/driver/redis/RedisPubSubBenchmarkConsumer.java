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

import com.fasterxml.jackson.databind.ObjectReader;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisPubSubBenchmarkConsumer implements BenchmarkConsumer {
    private final Integer consumerId;
    private final String topic;
    private final ObjectReader reader;
    private final List<StatefulRedisPubSubConnection<String, byte[]>> pool;
    private final ConsumerCallback consumerCallback;
    private final RedisPubSubAdapter<String, byte[]> listener;

    public RedisPubSubBenchmarkConsumer(
            final Integer consumerId,
            final String topic,
            final ObjectReader reader,
            final List<StatefulRedisPubSubConnection<String, byte[]>> pool,
            ConsumerCallback consumerCallback) {
        this.consumerId = consumerId;
        this.topic = topic;
        this.reader = reader;
        this.pool = pool;
        this.consumerCallback = consumerCallback;
        this.listener =
                new RedisPubSubAdapter<String, byte[]>() {
                    @Override
                    public void message(String channel, byte[] message) {
                        Map<String, byte[]> map;
                        try {
                            map = reader.readValue(message);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        byte[] payload = map.get("payload");
                        long timestamp = 0;
                        for (int i = 0; i < Long.BYTES; i++) {
                            timestamp = (timestamp << 8) + (payload[i] & 0xFF);
                        }
                        consumerCallback.messageReceived(payload, timestamp);
                    }
                };

        try {
            StatefulRedisPubSubConnection<String, byte[]> conn =
                    this.pool.get(this.consumerId % pool.size());
            conn.addListener(this.listener);
            RedisPubSubCommands<String, byte[]> commands = conn.sync();
            commands.subscribe(this.topic);
        } catch (Exception e) {
            log.error("Failed to read from consumer instance.", e);
        }
    }

    @Override
    public void close() throws Exception {
        StatefulRedisPubSubConnection<String, byte[]> conn =
                this.pool.get(this.consumerId % pool.size());
        conn.sync().unsubscribe(this.topic);
        conn.removeListener(this.listener);
    }

    private static final Logger log = LoggerFactory.getLogger(RedisPubSubBenchmarkDriver.class);
}
