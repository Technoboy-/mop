/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.support.systemtopic;

import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.ActionType;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.MoPEvent;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * System topic for client id.
 */
public class ClientIdSystemTopicClient extends SystemTopicClientBase<MoPEvent> {

    public ClientIdSystemTopicClient(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected  CompletableFuture<Writer<MoPEvent>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(MoPEvent.class))
                .topic(topicName.toString())
                .enableBatching(false)
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new ClientIdWriter(producer,
                            ClientIdSystemTopicClient.this));
                });
    }

    @Override
    protected CompletableFuture<Reader<MoPEvent>> newReaderAsyncInternal() {
        return client.newReader(Schema.AVRO(MoPEvent.class))
                .topic(topicName.toString())
                .startMessageId(MessageId.earliest)
                .readCompacted(true).createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new ClientIdReader(reader,
                            ClientIdSystemTopicClient.this));
                });
    }

    private static class ClientIdWriter implements Writer<MoPEvent> {

        private final Producer<MoPEvent> producer;
        private final org.apache.pulsar.broker.systopic.SystemTopicClient<MoPEvent> systemTopicClient;

        private ClientIdWriter(Producer<MoPEvent> producer, org.apache.pulsar.broker.systopic.SystemTopicClient<MoPEvent> systemTopicClient) {
            this.producer = producer;
            this.systemTopicClient = systemTopicClient;
        }

        @Override
        public MessageId write(MoPEvent event) throws PulsarClientException {
            TypedMessageBuilder<MoPEvent> builder = producer.newMessage().value(event);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(MoPEvent event) {
            TypedMessageBuilder<MoPEvent> builder = producer.newMessage().value(event);
            return builder.sendAsync();
        }

        @Override
        public MessageId delete(MoPEvent event) throws PulsarClientException {
            validateActionType(event);
            TypedMessageBuilder<MoPEvent> builder = producer.newMessage().value(null);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(MoPEvent event) {
            validateActionType(event);
            TypedMessageBuilder<MoPEvent> builder = producer.newMessage().value(null);
            return builder.sendAsync();
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
            systemTopicClient.getWriters().remove(ClientIdWriter.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                systemTopicClient.getWriters().remove(ClientIdWriter.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public org.apache.pulsar.broker.systopic.SystemTopicClient<MoPEvent> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    private static void validateActionType(MoPEvent event) {
        if (event == null || !ActionType.DELETE.equals(event.getActionType())) {
            throw new UnsupportedOperationException("The only supported ActionType is DELETE");
        }
    }

    private static class ClientIdReader implements Reader<MoPEvent> {

        private final org.apache.pulsar.client.api.Reader<MoPEvent> reader;
        private final ClientIdSystemTopicClient systemTopic;

        private ClientIdReader(org.apache.pulsar.client.api.Reader<MoPEvent> reader,
                                  ClientIdSystemTopicClient systemTopic) {
            this.reader = reader;
            this.systemTopic = systemTopic;
        }

        @Override
        public Message<MoPEvent> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<MoPEvent>> readNextAsync() {
            return reader.readNextAsync();
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            return reader.hasMessageAvailable();
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return reader.hasMessageAvailableAsync();
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
            systemTopic.getReaders().remove(ClientIdReader.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return reader.closeAsync().thenCompose(v -> {
                systemTopic.getReaders().remove(ClientIdReader.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<MoPEvent> getSystemTopic() {
            return systemTopic;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClientIdSystemTopicClient.class);
}
