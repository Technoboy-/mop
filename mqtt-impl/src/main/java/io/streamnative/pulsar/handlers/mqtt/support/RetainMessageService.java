package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.utils.MessagePublishContext;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.util.FutureUtil;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter.toPulsarMsg;

public class RetainMessageService {

    private final PulsarService pulsarService;

    private final MQTTServerConfiguration configuration;

    public RetainMessageService(PulsarService pulsarService, MQTTServerConfiguration configuration) {
        this.pulsarService = pulsarService;
        this.configuration = configuration;
    }

    public boolean isRetainMessage(MqttPublishMessage msg) {
        return msg.fixedHeader().isRetain();
    }

    public CompletableFuture<Void> doRetain(MqttPublishMessage msg) {
        if(!isRetainMessage(msg)) {
            return FutureUtils.Void();
        }
        CompletableFuture<Void> removeFuture = null;
        if (msg.fixedHeader().qosLevel().equals(MqttQoS.AT_MOST_ONCE) || msg.payload().capacity() == 0) {
            removeFuture = removePrevious(msg.variableHeader().topicName());
            if (msg.payload().capacity() == 0) {
                return removeFuture;
            }
        }
        if (removeFuture != null) {
            return removeFuture.thenAccept(__ -> writeRetainMessage(msg));
        }
        return FutureUtils.Void();
    }

    public CompletableFuture<Boolean> checkRetain(MqttPublishMessage msg) {
        if(!isRetainMessage(msg)) {
            return FutureUtils.value(true);
        }
        if (msg.fixedHeader().qosLevel().equals(MqttQoS.AT_MOST_ONCE) || msg.payload().capacity() == 0) {
            CompletableFuture<Void> removeFuture = removePrevious(msg.variableHeader().topicName());
            if (msg.payload().capacity() == 0) {
                return removeFuture.thenCompose(__ -> FutureUtils.value(false));
            }
        }
        return FutureUtils.value(true);
    }

    public CompletableFuture<Void> removePrevious(String topicName) {
        CompletableFuture<Optional<Topic>> retainTopicFuture = getRetainTopic(topicName);
        return retainTopicFuture.thenCompose(topicOp -> {
            if (topicOp.isPresent()) {
                try {
                    PulsarAdmin adminClient = pulsarService.getAdminClient();
                    return adminClient.topics().expireMessagesForAllSubscriptionsAsync(topicName, 0);
                } catch (PulsarServerException e) {
                    return FutureUtils.exception(e);
                }
            }
            return FutureUtils.Void();
        });
    }

    protected CompletableFuture<PositionImpl> writeRetainMessage(MqttPublishMessage msg) {
        return getRetainTopic(msg.variableHeader().topicName()).thenCompose(topicOp -> {
            CompletableFuture<PositionImpl> pos = topicOp.map(topic -> {
                MessageImpl<byte[]> message = toPulsarMsg(topic, msg);
                CompletableFuture<PositionImpl> ret = MessagePublishContext.publishMessages(message, topic);
                message.release();
                return ret;
            }).orElseGet(() -> FutureUtil.failedFuture(
                    new BrokerServiceException.TopicNotFoundException(msg.variableHeader().topicName())));

            return pos;
        });
    }

    private CompletableFuture<Optional<Topic>> getRetainTopic(String topicName) {
        return PulsarTopicUtils.getTopicReference(pulsarService, topicName,
                configuration.getDefaultTenant(), configuration.getDefaultNamespace(), true,
                configuration.getDefaultTopicDomain(), false);
    }
}
