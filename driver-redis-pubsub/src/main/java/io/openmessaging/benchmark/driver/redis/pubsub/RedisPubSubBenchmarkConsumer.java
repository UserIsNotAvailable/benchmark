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
package io.openmessaging.benchmark.driver.redis.pubsub;

import com.fasterxml.jackson.databind.ObjectReader;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisPubSubBenchmarkConsumer extends RedisPubSubAdapter<String, byte[]>
        implements BenchmarkConsumer {
    private final Integer consumerId;
    private final String topic;
    private final ObjectReader reader;
    private final StatefulRedisPubSubConnection<String, byte[]> conn;
    private final ConsumerCallback consumerCallback;

    public RedisPubSubBenchmarkConsumer(
            final Integer consumerId,
            final String topic,
            final ObjectReader reader,
            final StatefulRedisPubSubConnection<String, byte[]> conn,
            ConsumerCallback consumerCallback) {
        this.consumerId = consumerId;
        this.topic = topic;
        this.reader = reader;
        this.conn = conn;
        this.consumerCallback = consumerCallback;

        try {
            this.conn.addListener(this);
            RedisPubSubCommands<String, byte[]> commands = this.conn.sync();
            commands.subscribe(this.topic);
        } catch (Exception e) {
            log.error("Failed to read from consumer instance.", e);
        }
    }

    @Override
    public void message(String channel, byte[] message) {
        if (!topic.equals(channel)) {
            return;
        }
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

    @Override
    public void close() throws Exception {
        this.conn.sync().unsubscribe(this.topic);
        this.conn.removeListener(this);
    }

    private static final Logger log = LoggerFactory.getLogger(RedisPubSubBenchmarkDriver.class);
}