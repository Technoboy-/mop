package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SNCloudTokenProducer {

    public static void main(String[] args) {
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
        byte[] msg = "Hello MQTT".getBytes(StandardCharsets.UTF_8);
        client.publishWith()
                .topic(topicName)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        client.unsubscribeWith().topicFilter(topicName).send();
        client.disconnect();
    }
}
