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

import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.EventType;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events.EventsTopicNames;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class NamespaceEventsSystemTopicFactory {

    private final PulsarClient client;

    public NamespaceEventsSystemTopicFactory(PulsarClient client) {
        this.client = client;
    }

    public ClientIdSystemTopicClient createClientIdSystemTopicClient() {
        TopicName topicName = TopicName.get(TopicDomain.non_persistent.value(), NamespaceName.get("mop/system"),
                EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
        log.info("Create topic policies system topic client {}", topicName.toString());
        return new ClientIdSystemTopicClient(client, topicName);
    }

    public static TopicName getSystemTopicName(EventType eventType) {
        switch (eventType) {
            case CLIENT_ID:
                return TopicName.get(TopicDomain.non_persistent.value(), NamespaceName.get("mop/system"),
                        EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
            default:
                return null;
        }
    }
}
