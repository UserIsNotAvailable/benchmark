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

import static java.net.HttpURLConnection.HTTP_OK;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.asynchttpclient.AsyncHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxBenchmarkProducer implements BenchmarkProducer {
    private final String topic;
    private final String path;
    private final AsyncHttpClient httpClient;

    private final String sendType;

    ObjectMapper objectMapper = new ObjectMapper();

    public VertxBenchmarkProducer(
            final String topic,
            final String path,
            final AsyncHttpClient httpClient,
            final String sendType) {
        this.topic = topic;
        this.path = path;
        this.httpClient = httpClient;
        this.sendType = sendType;
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {

        Map<String, Object> data = new HashMap<>();
        data.put("payload", Base64.getEncoder().encodeToString(payload));
        data.put("ts", System.currentTimeMillis());
        String body = "";
        try {
            body = objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            log.error(e.toString());
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        httpClient
                .preparePost(this.path)
                .setHeader("spcls", this.sendType)
                .setHeader("uc", topic)
                .setBody(body)
                .execute()
                .toCompletableFuture()
                .thenApply(
                        response -> {
                            if (response.getStatusCode() == HTTP_OK) {
                                future.complete(null);
                            } else {
                                future.completeExceptionally(new Exception());
                            }
                            return (Void) null;
                        })
                .join();

        return future;
    }

    @Override
    public void close() throws Exception {
        // Close in Driver
    }

    private static final Logger log = LoggerFactory.getLogger(VertxBenchmarkDriver.class);
}
