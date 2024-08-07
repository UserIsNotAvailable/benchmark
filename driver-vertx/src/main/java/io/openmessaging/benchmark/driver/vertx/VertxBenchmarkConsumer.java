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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxBenchmarkConsumer implements BenchmarkConsumer {

    private Channel ch;

    public VertxBenchmarkConsumer(
            final EventLoopGroup group,
            final URI uri,
            final String topic,
            ConsumerCallback consumerCallback) {

        final WebSocketClientHandler handler =
                new WebSocketClientHandler(
                        WebSocketClientHandshakerFactory.newHandshaker(
                                uri, WebSocketVersion.V13, "push," + topic, true, new DefaultHttpHeaders()),
                        consumerCallback);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(
                        new ChannelInitializer() {
                            @Override
                            protected void initChannel(Channel ch) {
                                ChannelPipeline p = ch.pipeline();
                                p.addLast(
                                        new HttpClientCodec(),
                                        new HttpObjectAggregator(8192),
                                        WebSocketClientCompressionHandler.INSTANCE,
                                        handler);
                            }
                        });
        try {
            this.ch = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
        } catch (Exception e) {
            log.error("hander exception:{}", e.toString());
        }
    }

    private static HttpHeaders createCustomHeaders(String topic) {
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "push," + topic);
        // headers.add(HttpHeaderNames.AUTHORIZATION, topic);
        return headers;
    }

    @Override
    public void close() throws Exception {
        ch.close();
    }

    private static final Logger log = LoggerFactory.getLogger(VertxBenchmarkDriver.class);

    private static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

        private final WebSocketClientHandshaker handshaker;

        private ChannelPromise handshakeFuture;

        private final ConsumerCallback consumerCallback;

        private final ObjectMapper objectMapper = new ObjectMapper();

        public WebSocketClientHandler(
                WebSocketClientHandshaker handshaker, ConsumerCallback consumerCallback) {
            this.handshaker = handshaker;
            this.consumerCallback = consumerCallback;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
            // ctx.writeAndFlush(
            //        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/push/server"));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.debug("WebSocket Client disconnected!");
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    log.debug("WebSocket Client connected!");
                    handshakeFuture.setSuccess();
                } catch (WebSocketHandshakeException e) {
                    log.error("WebSocket Client failed to connect");
                    log.error(e.toString());
                    handshakeFuture.setFailure(e);
                }
                return;
            }

            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException(
                        "Unexpected FullHttpResponse (getStatus="
                                + response.status()
                                + ", content="
                                + response.content().toString(io.netty.util.CharsetUtil.UTF_8)
                                + ')');
            } else if (msg instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) msg;
                if (frame instanceof TextWebSocketFrame) {
                    TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                    try {
                        Map<String, Object> decodedMap = objectMapper.readValue(textFrame.text(), Map.class);
                        byte[] payload = Base64.getDecoder().decode((String) decodedMap.get("payload"));
                        long timestamp = (long) decodedMap.get("ts");
                        consumerCallback.messageReceived(payload, timestamp);
                    } catch (Exception e) {
                        log.error(e.toString());
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
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            if (!handshakeFuture.isDone()) {
                handshakeFuture.setFailure(cause);
            }
            ctx.close();
        }

        public ChannelFuture handshakeFuture() {
            return handshakeFuture;
        }
    }
}
