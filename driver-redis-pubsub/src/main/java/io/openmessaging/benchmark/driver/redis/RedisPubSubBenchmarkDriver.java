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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StaticCredentialsProvider;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.redis.client.RedisClientConfig;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisPubSubBenchmarkDriver implements BenchmarkDriver {
    private RedisClientConfig clientConfig;
    private RedisClient redisClient;
    private final AtomicInteger consumerIds = new AtomicInteger(0);
    private final AtomicInteger producerIds = new AtomicInteger(0);
    private final ObjectMapper m = new ObjectMapper();
    private final ObjectWriter writer = m.writerFor(new TypeReference<HashMap<String, byte[]>>() {});
    private final ObjectReader reader = m.readerFor(new TypeReference<HashMap<String, byte[]>>() {});
    private List<StatefulRedisPubSubConnection<String, byte[]>> lettucePool;

    @Override
    public void initialize(final File configurationFile, final StatsLogger statsLogger)
            throws IOException {
        this.clientConfig = readConfig(configurationFile);
    }

    @Override
    public String getTopicNamePrefix() {
        return "redis-pubsub-openmessaging-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(final String topic, final int partitions) {
        return CompletableFuture.runAsync(() -> {});
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        int producerId = producerIds.getAndIncrement();
        if (lettucePool == null) {
            setupLettuceConn();
        }
        return CompletableFuture.completedFuture(
                new RedisPubSubBenchmarkProducer(producerId, topic, this.writer, this.lettucePool));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        int consumerId = consumerIds.getAndIncrement();
        if (lettucePool == null) {
            setupLettuceConn();
        }
        return CompletableFuture.completedFuture(
                new RedisPubSubBenchmarkConsumer(
                        consumerId, topic, this.reader, this.lettucePool, consumerCallback));
    }

    private void setupLettuceConn() {
        RedisURI redisUri =
                RedisURI.builder()
                        .withHost(this.clientConfig.redisHost)
                        .withPort(this.clientConfig.redisPort)
                        .withTimeout(Duration.ofMillis(2000))
                        .build();

        if (this.clientConfig.redisPass != null) {
            if (this.clientConfig.redisUser != null) {
                redisUri.setCredentialsProvider(
                        new StaticCredentialsProvider(
                                this.clientConfig.redisUser, this.clientConfig.redisPass.toCharArray()));
            } else {
                redisUri.setCredentialsProvider(
                        new StaticCredentialsProvider(null, this.clientConfig.redisPass.toCharArray()));
            }
        }

        this.redisClient = RedisClient.create(redisUri);

        lettucePool = new ArrayList<>(this.clientConfig.poolSize);

        for (int i = 0; i < this.clientConfig.poolSize; ++i) {
            lettucePool.add(
                    this.redisClient.connectPubSub(
                            new RedisCodec<String, byte[]>() {
                                private final StringCodec keyCodec = new StringCodec(StandardCharsets.UTF_8);
                                private final ByteArrayCodec valueCodec = new ByteArrayCodec();

                                @Override
                                public String decodeKey(ByteBuffer bytes) {
                                    return keyCodec.decodeKey(bytes);
                                }

                                @Override
                                public byte[] decodeValue(ByteBuffer bytes) {
                                    return valueCodec.decodeValue(bytes);
                                }

                                @Override
                                public ByteBuffer encodeKey(String key) {
                                    return keyCodec.encodeKey(key);
                                }

                                @Override
                                public ByteBuffer encodeValue(byte[] value) {
                                    return valueCodec.encodeValue(value);
                                }
                            },
                            redisUri));
        }
    }

    @Override
    public void close() throws Exception {
        if (this.lettucePool != null) {
            this.lettucePool.forEach(StatefulConnection::close);
            this.lettucePool = null;
        }
        if (this.redisClient != null) {
            this.redisClient.close();
            this.redisClient = null;
        }
    }

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static RedisClientConfig readConfig(File configurationFile) throws IOException {
        return mapper.readValue(configurationFile, RedisClientConfig.class);
    }

    private static final Logger log = LoggerFactory.getLogger(RedisPubSubBenchmarkDriver.class);
}
