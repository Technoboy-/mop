/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Subscription manager.
 */
@Slf4j
public class MQTTSubscriptionManager {

    private ConcurrentMap<String, List<MqttTopicSubscription>> subscriptions = new ConcurrentHashMap<>(2048);

    private ConcurrentMap<String, List<TopicName>> subTopics = new ConcurrentHashMap<>(2048);

    private MQTTNamespaceBundleOwnershipListener bundleOwnershipListener;

    private MQTTConnectionManager connectionManager;

    public MQTTSubscriptionManager(MQTTNamespaceBundleOwnershipListener bundleOwnershipListener,
                                   MQTTConnectionManager connectionManager) {
        this.bundleOwnershipListener = bundleOwnershipListener;
        this.connectionManager = connectionManager;
        this.bundleOwnershipListener.addListener(new DefaultMQTTTopicOwnershipListener());
    }

    public void addSubscriptions(String clientId, List<MqttTopicSubscription> topicSubscriptions) {
        this.subscriptions.computeIfAbsent(clientId, k -> new ArrayList<>()).addAll(topicSubscriptions);
    }

    public void addSubTopics(String clientId, List<String> topics) {
        subTopics.computeIfAbsent(clientId, k -> topics.stream().map(TopicName::get).collect(Collectors.toList()));
    }

    public List<Pair<String, String>> findMatchTopic(String topic) {
        List<Pair<String, String>> result = new ArrayList<>();
        Set<Map.Entry<String, List<MqttTopicSubscription>>> entries = subscriptions.entrySet();
        for (Map.Entry<String, List<MqttTopicSubscription>> entry : entries) {
            String clientId = entry.getKey();
            List<MqttTopicSubscription> subs = entry.getValue();
            subs.forEach(sub -> {
                if (new TopicFilterImpl(sub.topicName()).test(topic)) {
                    result.add(Pair.of(clientId, sub.topicName()));
                }
            });
        }
        return result;
    }

    public void removeSubscription(String clientId) {
        subscriptions.remove(clientId);
    }

    class DefaultMQTTTopicOwnershipListener implements MQTTTopicOwnershipListener {

        @Override
        public void load(TopicName topicName) {

        }

        @Override
        public void unload(TopicName topicName) {
            Set<String> clientIds = new HashSet<>();
            subTopics.entrySet().forEach(entry -> {
                String clientId = entry.getKey();
                List<TopicName> topicNames = entry.getValue();
                topicNames.forEach(topic -> {
                    if (topic.equals(topicName)) {
                        clientIds.add(clientId);
                    }
                });
            });
            clientIds.forEach(clientId -> {
                Connection connection = connectionManager.getConnection(clientId);
                if (connection != null) {
                    connection.setFenced();
                    connection.close();
                    connectionManager.removeConnection(connection);
                }
            });
        }
    }
}
