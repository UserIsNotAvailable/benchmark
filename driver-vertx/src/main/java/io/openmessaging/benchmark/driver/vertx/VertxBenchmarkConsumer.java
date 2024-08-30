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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxBenchmarkConsumer implements BenchmarkConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final EventLoopGroup group;
    private final String topic;
    private final URI uri;
    private final ConsumerCallback callback;

    private Channel ch;
    private WebSocketClientHandler handler;
    private volatile boolean reconnect = false;

    public static CompletableFuture<VertxBenchmarkConsumer> create(
            final EventLoopGroup group,
            final URI uri,
            final String topic,
            final ConsumerCallback consumerCallback) {
        final VertxBenchmarkConsumer consumer =
                new VertxBenchmarkConsumer(group, uri, topic, consumerCallback);
        return consumer.open().thenApply(unused -> consumer);
    }

    private VertxBenchmarkConsumer(
            final EventLoopGroup group,
            final URI uri,
            final String topic,
            final ConsumerCallback consumerCallback) {
        this.group = group;
        this.topic = topic;
        this.uri = uri;
        this.callback = consumerCallback;
    }

    private CompletableFuture<Void> open() {
        this.handler =
                new WebSocketClientHandler(
                        this,
                        WebSocketClientHandshakerFactory.newHandshaker(
                                this.uri,
                                WebSocketVersion.V13,
                                "push," + this.topic,
                                true,
                                new DefaultHttpHeaders()));

        final Bootstrap bootstrap = new Bootstrap();
        bootstrap
                .group(this.group)
                .channel(NioSocketChannel.class)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(final SocketChannel ch) {
                                ch.pipeline()
                                        .addLast(
                                                new HttpClientCodec(),
                                                new HttpObjectAggregator(8192),
                                                WebSocketClientCompressionHandler.INSTANCE,
                                                VertxBenchmarkConsumer.this.handler);
                            }
                        });

        final CompletableFuture<Void> ret = new CompletableFuture<>();
        try {
            this.ch = bootstrap.connect(this.uri.getHost(), this.uri.getPort()).sync().channel();
            this.handler
                    .handshakeFuture()
                    .addListener(
                            result -> {
                                if (result.isSuccess()) {
                                    ret.complete(null);
                                } else {
                                    ret.completeExceptionally(result.cause());
                                }
                            });
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            ret.completeExceptionally(e);
        }
        return ret;
    }

    @Override
    public void close() throws Exception {
        this.reconnect = false;
        if (null != this.ch) {
            this.ch.writeAndFlush(new CloseWebSocketFrame()).await();
            this.ch.closeFuture().await();
            this.ch = null;
        }
        if (null != this.handler) {
            this.handler = null;
        }
    }

    private void consume(final String text) throws JsonProcessingException {
        final Map<String, Object> decodedMap = this.objectMapper.readValue(text, Map.class);
        final byte[] payload = Base64.getDecoder().decode((String) decodedMap.get("payload"));
        final long timestamp = (long) decodedMap.get("ts");
        this.callback.messageReceived(payload, timestamp);
    }

    private void channelActive() {
        this.reconnect = true;
    }

    private void channelInactive() {
        log.debug("WebSocket Client dreconnected!");
        if (this.reconnect) {
            try {
                this.close();
                this.open().get();
                log.debug("WebSocket Client reconnect!");
            } catch (final Exception e) {
                log.error("WebSocket Client reconnect failed", e);
            }
        }
    }

    private static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;
        private final VertxBenchmarkConsumer consumer;

        public WebSocketClientHandler(
                final VertxBenchmarkConsumer consumer, final WebSocketClientHandshaker handshaker) {
            this.consumer = consumer;
            this.handshaker = handshaker;
        }

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) {
            this.handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            this.handshaker.handshake(ctx.channel());
            this.consumer.channelActive();
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
            this.consumer.channelInactive();
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) {
            final Channel ch = ctx.channel();
            if (!this.handshaker.isHandshakeComplete()) {
                try {
                    this.handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    log.debug("WebSocket Client connected!");
                    this.handshakeFuture.setSuccess();
                } catch (final WebSocketHandshakeException e) {
                    log.error("WebSocket Client failed to connect", e);
                    this.handshakeFuture.setFailure(e);
                }
                return;
            }

            if (msg instanceof final FullHttpResponse response) {
                throw new IllegalStateException(
                        "Unexpected FullHttpResponse (getStatus="
                                + response.status()
                                + ", content="
                                + response.content().toString(io.netty.util.CharsetUtil.UTF_8)
                                + ')');
            } else if (msg instanceof final WebSocketFrame frame) {
                if (frame instanceof final TextWebSocketFrame textFrame) {
                    try {
                        this.consumer.consume(textFrame.text());
                    } catch (final Exception e) {
                        log.error(e.getMessage(), e);
                    }
                } else if (frame instanceof PongWebSocketFrame) {
                    log.debug("WebSocket Client received pong");
                } else if (frame instanceof CloseWebSocketFrame) {
                    log.debug("WebSocket Client received closing");
                    ctx.close();
                }
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (!this.handshakeFuture.isDone()) {
                this.handshakeFuture.setFailure(cause);
            }
            ctx.close();
            log.error(cause.getMessage(), cause);
        }

        public ChannelFuture handshakeFuture() {
            return this.handshakeFuture;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(VertxBenchmarkConsumer.class);
}
