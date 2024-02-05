package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SNCloudTokenConsumer {

    public static void main(String[] args) throws Exception {
        final String jwtToken = "${token}";
        final String userName = "${userName}";
        final String topicName = "${tenant}/${namespace}/${topic}";
        final String serverUrl = "${mopServiceURL}";
        final String serverHost = serverUrl.split(":")[0];
        final int serverPort = Integer.parseInt(serverUrl.split(":")[1]);
        Mqtt5SimpleAuth simpleAuth = Mqtt5SimpleAuth.builder().username(userName)
                .password(jwtToken.getBytes())
                .build();
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(serverHost)
                .serverPort(serverPort)
                .simpleAuth(simpleAuth)
                .buildBlocking();
        client.connect();
        client.subscribeWith().topicFilter(topicName).qos(MqttQos.AT_LEAST_ONCE).send();
        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        boolean running = true;
        while (running) {
            Optional<Mqtt5Publish> publish = publishes.receive(10, TimeUnit.SECONDS);
            if (publish.isEmpty()) {
                break;
            }
            final Mqtt5Publish msg = publish.get();
            System.out.println("Receive record: topic : " + msg.getTopic() + ", value : "
                    + new String(msg.getPayloadAsBytes()));
        }
        client.unsubscribeWith().topicFilter(topicName).send();
        client.disconnect();
    }
}
