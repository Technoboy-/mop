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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.ActionType;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.ConnectionEvent;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.EventType;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.MoPEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.TopicPolicyListener;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.util.RetryUtil;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cached topic policies service will cache the system topic reader and the topic policies
 *
 * While reader cache for the namespace was removed, the topic policies will remove automatically.
 */
@Slf4j
public class SystemTopicBasedClientIdService {

    private final PulsarService pulsarService;
    private volatile NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    @VisibleForTesting
    final Map<TopicName, TopicPolicies> policiesCache = new ConcurrentHashMap<>();

    private final Map<NamespaceName, AtomicInteger> ownedBundlesCountPerNamespace = new ConcurrentHashMap<>();

    private final Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<MoPEvent>>>
            readerCaches = new ConcurrentHashMap<>();
    @VisibleForTesting
    final Map<NamespaceName, Boolean> policyCacheInitMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners = new ConcurrentHashMap<>();

    public SystemTopicBasedClientIdService(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
    }

    private CompletableFuture<Void> sendConnectEvent() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            createSystemTopicFactoryIfNeeded();
        } catch (PulsarServerException e) {
            result.completeExceptionally(e);
            return result;
        }
        SystemTopicClient<MoPEvent> systemTopicClient =
                namespaceEventsSystemTopicFactory.createClientIdSystemTopicClient();
        CompletableFuture<SystemTopicClient.Writer<MoPEvent>> writerFuture = systemTopicClient.newWriterAsync();
        writerFuture.whenComplete((writer, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                MoPEvent event = getMoPEvent();
                CompletableFuture<MessageId> actionFuture = writer.writeAsync(event);
                actionFuture.whenComplete(((messageId, e) -> {
                            if (e != null) {
                                result.completeExceptionally(e);
                            } else {
                                if (messageId != null) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(new RuntimeException("Got message id is null."));
                                }
                            }
                            writer.closeAsync().whenComplete((v, cause) -> {
                                if (cause != null) {
                                    log.error("Close writer error.", cause);
                                }
                            });
                        })
                );
            }
        });
        return result;
    }

    private MoPEvent getMoPEvent() {
        MoPEvent.MoPEventBuilder builder = MoPEvent.builder();
        return builder
                .actionType(ActionType.INSERT)
                .eventType(EventType.CLIENT_ID)
                .connectionEvent(ConnectionEvent.builder().hostAndPort("").build())
                .build();
    }

    private void notifyListener(Message<MoPEvent> msg) {
        // delete policies
        if (msg.getValue() == null) {
            TopicName topicName =  TopicName.get(TopicName.get(msg.getKey()).getPartitionedTopicName());
            if (listeners.get(topicName) != null) {
                for (TopicPolicyListener<TopicPolicies> listener : listeners.get(topicName)) {
                    listener.onUpdate(null);
                }
            }
            return;
        }
        if (!EventType.CLIENT_ID.equals(msg.getValue().getEventType())) {
            return;
        }
        ConnectionEvent event = msg.getValue().getConnectionEvent();
    }

    protected CompletableFuture<SystemTopicClient.Reader<MoPEvent>> creatSystemTopicClientWithRetry() {
        CompletableFuture<SystemTopicClient.Reader<MoPEvent>> result = new CompletableFuture<>();
        try {
            createSystemTopicFactoryIfNeeded();
        } catch (PulsarServerException e) {
            result.completeExceptionally(e);
            return result;
        }
        SystemTopicClient<MoPEvent> systemTopicClient = namespaceEventsSystemTopicFactory
                .createClientIdSystemTopicClient();
        Backoff backoff = new Backoff(1, TimeUnit.SECONDS, 3, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
        RetryUtil.retryAsynchronously(() -> {
            try {
                return systemTopicClient.newReader();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }, backoff, pulsarService.getExecutor(), result);
        return result;
    }

    public void start() {

    }

    private void initCache(SystemTopicClient.Reader<MoPEvent> reader, CompletableFuture<Void> future) {
        reader.hasMoreEventsAsync().whenComplete((hasMore, ex) -> {
            if (ex != null) {
                log.error("[{}] Failed to check the move events for the system topic",
                        reader.getSystemTopic().getTopicName(), ex);
                future.completeExceptionally(ex);
                readerCaches.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                policyCacheInitMap.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                reader.closeAsync();
                return;
            }
            if (hasMore) {
                reader.readNextAsync().whenComplete((msg, e) -> {
                    if (e != null) {
                        log.error("[{}] Failed to read event from the system topic.",
                                reader.getSystemTopic().getTopicName(), ex);
                        future.completeExceptionally(e);
                        readerCaches.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                        policyCacheInitMap.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                        reader.closeAsync();
                        return;
                    }
                    refreshCache(msg);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Loop next event reading for system topic.",
                                reader.getSystemTopic().getTopicName().getNamespaceObject());
                    }
                    initCache(reader, future);
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Reach the end of the system topic.", reader.getSystemTopic().getTopicName());
                }
                policyCacheInitMap.computeIfPresent(
                        reader.getSystemTopic().getTopicName().getNamespaceObject(), (k, v) -> true);
                // replay policy message
                policiesCache.forEach(((topicName, topicPolicies) -> {
                    if (listeners.get(topicName) != null) {
                        for (TopicPolicyListener<TopicPolicies> listener : listeners.get(topicName)) {
                            listener.onUpdate(topicPolicies);
                        }
                    }
                }));
                future.complete(null);
            }
        });
    }

    private void readMorePolicies(SystemTopicClient.Reader<MoPEvent> reader) {
        reader.readNextAsync().whenComplete((msg, ex) -> {
            if (ex == null) {
                refreshCache(msg);
                notifyListener(msg);
                readMorePolicies(reader);
            } else {
                if (ex instanceof PulsarClientException.AlreadyClosedException) {
                    log.error("Read more topic policies exception, close the read now!", ex);
                    NamespaceName namespace = reader.getSystemTopic().getTopicName().getNamespaceObject();
                    ownedBundlesCountPerNamespace.remove(namespace);
                    readerCaches.remove(namespace);
                } else {
                    readMorePolicies(reader);
                }
            }
        });
    }

    private void refreshCache(Message<MoPEvent> msg) {
        // delete policies
        if (msg.getValue() == null) {
            return;
        }
        if (EventType.CLIENT_ID.equals(msg.getValue().getEventType())) {
            ConnectionEvent event = msg.getValue().getConnectionEvent();
            switch (msg.getValue().getActionType()) {
                case INSERT:
                    String hostAndPort = event.getHostAndPort();
                    break;
                default:
                    log.warn("Unknown event action type: {}", msg.getValue().getActionType());
                    break;
            }
        }
    }

    private void createSystemTopicFactoryIfNeeded() throws PulsarServerException {
        if (namespaceEventsSystemTopicFactory == null) {
            synchronized (this) {
                if (namespaceEventsSystemTopicFactory == null) {
                    try {
                        namespaceEventsSystemTopicFactory =
                                new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
                    } catch (PulsarServerException e) {
                        log.error("Create namespace event system topic factory error.", e);
                        throw e;
                    }
                }
            }
        }
    }

    public void registerListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
        listeners.compute(topicName, (k, topicListeners) -> {
            if (topicListeners == null) {
                topicListeners = Lists.newCopyOnWriteArrayList();
            }
            topicListeners.add(listener);
            return topicListeners;
        });
    }

    public void unregisterListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
        listeners.compute(topicName, (k, topicListeners) -> {
            if (topicListeners != null){
                topicListeners.remove(listener);
                if (topicListeners.isEmpty()) {
                    topicListeners = null;
                }
            }
            return topicListeners;
        });
    }
}
