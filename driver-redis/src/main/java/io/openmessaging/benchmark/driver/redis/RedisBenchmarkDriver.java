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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.BaseEncoding;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StaticCredentialsProvider;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XGroupCreateArgs;
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
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisBenchmarkDriver implements BenchmarkDriver {
    private RedisClientConfig clientConfig;
    private RedisClient redisClient;
    private GenericObjectPool<StatefulRedisConnection<String, byte[]>> lettucePool;
    private final List<String> topics = new ArrayList<>();

    @Override
    public void initialize(final File configurationFile, final StatsLogger statsLogger)
            throws IOException {
        this.clientConfig = readConfig(configurationFile);
    }

    @Override
    public String getTopicNamePrefix() {
        return "redis-openmessaging-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(final String topic, final int partitions) {
        return CompletableFuture.runAsync(() -> {});
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        if (lettucePool == null) {
            setupLettuceConn();
        }
        return CompletableFuture.completedFuture(new RedisBenchmarkProducer(lettucePool, topic));
    }

    @Override
    public CompletableFuture<List<BenchmarkConsumer>> createConsumers(List<ConsumerInfo> consumers) {
        List<CompletableFuture<BenchmarkConsumer>> futures =
                consumers.stream()
                        .peek(ci -> this.topics.add(ci.getTopic()))
                        .map(
                                ci ->
                                        createConsumer(
                                                ci.getTopic(), ci.getSubscriptionName(), ci.getConsumerCallback()))
                        .toList()
                        .stream()
                        .map(
                                future ->
                                        CompletableFuture.supplyAsync(
                                                () -> {
                                                    try {
                                                        RedisBenchmarkConsumer consumer = ((RedisBenchmarkConsumer) future.get());
                                                        consumer.start();
                                                        return (BenchmarkConsumer) consumer;
                                                    } catch (Exception e) {
                                                        log.info("Failed to create consumer instance.", e);
                                                    }
                                                    return null;
                                                }))
                        .toList();
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).toList());
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        String consumerId = "consumer-" + getRandomString();
        if (lettucePool == null) {
            setupLettuceConn();
        }
        try (StatefulRedisConnection<String, byte[]> conn = this.lettucePool.borrowObject()) {
            RedisCommands<String, byte[]> commands = conn.sync();
            commands.xgroupCreate(
                    XReadArgs.StreamOffset.latest(topic),
                    subscriptionName,
                    XGroupCreateArgs.Builder.mkstream());
        } catch (RedisBusyException e) {
            // Ignore exist group
        } catch (Exception e) {
            log.info("Failed to create consumer instance.", e);
        }
        return CompletableFuture.completedFuture(
                new RedisBenchmarkConsumer(
                        consumerId, topic, subscriptionName, this.lettucePool, consumerCallback));
    }

    private void setupLettuceConn() {
        GenericObjectPoolConfig<StatefulRedisConnection<String, byte[]>> poolConfig =
                new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(this.clientConfig.jedisPoolMaxTotal);
        poolConfig.setMaxIdle(this.clientConfig.jedisPoolMaxIdle);

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
        this.lettucePool =
                ConnectionPoolSupport.createGenericObjectPool(
                        () ->
                                this.redisClient
                                        .connect(
                                                new RedisCodec<String, byte[]>() {
                                                    private final StringCodec keyCodec =
                                                            new StringCodec(StandardCharsets.UTF_8);
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
                                                redisUri),
                        poolConfig);
    }

    @Override
    public void close() throws Exception {
        if (this.lettucePool != null) {
            this.topics
                    .forEach(
                            topic -> {
                                try (StatefulRedisConnection<String, byte[]> conn = this.lettucePool.borrowObject()) {
                                    RedisCommands<String, byte[]> commands = conn.sync();
                                    commands.del(topic);
                                } catch (Exception e) {
                                    log.error("Failed to delete stream.", e);
                                }
                            });
            this.topics.clear();
            this.lettucePool.close();
        }
        if (this.redisClient != null) {
            this.redisClient.close();
        }
    }

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static RedisClientConfig readConfig(File configurationFile) throws IOException {
        return mapper.readValue(configurationFile, RedisClientConfig.class);
    }

    private static final Random random = new Random();

    private static String getRandomString() {
        byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return BaseEncoding.base64Url().omitPadding().encode(buffer);
    }

    private static final Logger log = LoggerFactory.getLogger(RedisBenchmarkDriver.class);
}
