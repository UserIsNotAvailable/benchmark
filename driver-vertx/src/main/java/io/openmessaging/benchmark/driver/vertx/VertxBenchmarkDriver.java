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
package io.openmessaging.benchmark.driver.vertx;

import static org.asynchttpclient.Dsl.asyncHttpClient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.vertx.client.VertxClientConfig;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.stats.StatsLogger;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxBenchmarkDriver implements BenchmarkDriver {
    private VertxClientConfig clientConfig;
    private EventLoopGroup eventLoopGroup;
    private AsyncHttpClient asyncHttpClient;
    private URI webSocketUri;

    private final AtomicLong topic = new AtomicLong(1);

    @Override
    public void initialize(final File configurationFile, final StatsLogger statsLogger)
            throws IOException {
        this.clientConfig = readConfig(configurationFile);
        this.eventLoopGroup = new NioEventLoopGroup();
        this.asyncHttpClient =
                asyncHttpClient(Dsl.config().setReadTimeout(1_800_000).setRequestTimeout(1_800_000));
        this.webSocketUri = null;
        try {
            this.webSocketUri = new URI(this.clientConfig.webSocketUrl);
        } catch (final Exception e) {
            log.error("format exception:{}({})", e, this.clientConfig.webSocketUrl);
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "vert-x";
    }

    @Override
    public CompletableFuture<String> createTopic(final String topic, final int partitions) {
        return CompletableFuture.supplyAsync(() -> String.valueOf(this.topic.getAndIncrement()));
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        return CompletableFuture.completedFuture(
                new VertxBenchmarkProducer(
                        topic + "0000000",
                        this.clientConfig.pushPath,
                        this.asyncHttpClient,
                        this.clientConfig.sendType));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        return VertxBenchmarkConsumer.create(
                        this.eventLoopGroup,
                        this.webSocketUri,
                        topic + subscriptionName.substring(4, 11),
                        consumerCallback)
                .thenApply(vertxBenchmarkConsumer -> vertxBenchmarkConsumer);
    }

    @Override
    public void close() throws Exception {
        this.asyncHttpClient.close();
        this.topic.set(1);
    }

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static VertxClientConfig readConfig(final File configurationFile) throws IOException {
        return mapper.readValue(configurationFile, VertxClientConfig.class);
    }

    private static final Logger log = LoggerFactory.getLogger(VertxBenchmarkDriver.class);
}
