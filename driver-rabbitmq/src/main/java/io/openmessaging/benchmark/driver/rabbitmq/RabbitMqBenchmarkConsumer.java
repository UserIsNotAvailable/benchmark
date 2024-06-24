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
package io.openmessaging.benchmark.driver.rabbitmq;

import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMqBenchmarkConsumer extends DefaultConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqBenchmarkConsumer.class);

    private final Channel channel;
    private final String exchange;
    private final String queueName;
    private final ConsumerCallback callback;

    public RabbitMqBenchmarkConsumer(
            Channel channel,
            String exchange,
            String queueName,
            Map<String, Object> arguments,
            ConsumerCallback callback)
            throws IOException {
        super(channel);

        this.channel = channel;
        this.exchange = exchange;
        this.callback = callback;
        this.queueName = queueName;
        this.channel.exchangeDeclare(this.exchange, BuiltinExchangeType.FANOUT, true);
        // Create the queue
        this.channel.queueDeclare(this.queueName, true, false, false, arguments);
        this.channel.queueBind(this.queueName, this.exchange, "");
        this.channel.basicConsume(this.queueName, true, this);
    }

    @Override
    public void handleDelivery(
            String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) {
        callback.messageReceived(body, properties.getTimestamp().getTime());
    }

    @Override
    public void close() throws Exception {
        try {
            channel.queueUnbind(this.queueName, this.exchange, "");
        } catch (Exception e) {
            log.warn("Queue delete error", e);
        }
        try {
            channel.queueDelete(this.queueName);
        } catch (Exception e) {
            log.warn("Queue delete error", e);
        }
        try {
            channel.exchangeDelete(this.exchange);
        } catch (Exception e) {
            log.warn("Exchange delete error", e);
        }
        try {
            channel.close();
        } catch (AlreadyClosedException e) {
            log.warn("Channel already closed", e);
        }
    }
}
