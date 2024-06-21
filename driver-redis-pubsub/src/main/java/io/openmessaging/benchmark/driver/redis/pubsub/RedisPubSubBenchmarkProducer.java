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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectWriter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class RedisPubSubBenchmarkProducer implements BenchmarkProducer {
    private final Integer producerId;
    private final String topic;
    private final ObjectWriter writer;
    private final StatefulRedisPubSubConnection<String, byte[]> conn;

    public RedisPubSubBenchmarkProducer(
            final Integer producerId,
            final String topic,
            final ObjectWriter writer,
            final StatefulRedisPubSubConnection<String, byte[]> conn) {
        this.producerId = producerId;
        this.topic = topic;
        this.writer = writer;
        this.conn = conn;
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {
        long timestamp = System.currentTimeMillis();
        byte[] bytes = new byte[Long.BYTES];
        int len = bytes.length;
        for (int i = 0; i < len; ++i) {
            bytes[len - i - 1] = (byte) (timestamp & 0xFF);
            timestamp >>= 8;
        }
        System.arraycopy(bytes, 0, payload, 0, len);

        Map<String, byte[]> map = new HashMap<>();
        map.put("payload", payload);

        if (key.isPresent()) {
            map.put("key", key.toString().getBytes(UTF_8));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RedisPubSubCommands<String, byte[]> commands = this.conn.sync();
            commands.publish(this.topic, this.writer.writeValueAsBytes(map));
            future.complete(null);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void close() throws Exception {
        // Close in Driver
    }
}
