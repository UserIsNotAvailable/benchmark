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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RedisBenchmarkProducer implements BenchmarkProducer {
    private final GenericObjectPool<StatefulRedisConnection<String, byte[]>> pool;
    private final String rmqTopic;

    public RedisBenchmarkProducer(
            final GenericObjectPool<StatefulRedisConnection<String, byte[]>> pool,
            final String rmqTopic) {
        this.pool = pool;
        this.rmqTopic = rmqTopic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {
        Map<String, byte[]> map1 = new HashMap<>();
        map1.put("payload", payload);

        if (key.isPresent()) {
            map1.put("key", key.toString().getBytes(UTF_8));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try (StatefulRedisConnection<String, byte[]> conn = this.pool.borrowObject()) {
            RedisCommands<String, byte[]> commands = conn.sync();
            commands.xadd(this.rmqTopic, map1);
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
