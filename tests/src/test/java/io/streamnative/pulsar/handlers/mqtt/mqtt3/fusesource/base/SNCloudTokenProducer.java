package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.base;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

public class SNCloudTokenProducer {

    public static void main(String[] args) throws Exception {
        final String jwtToken = "${token}";
        final String topicName = "${tenant}/${namespace}/${topic}";
        final String serverUrl = "${mopServiceURL}";
        final String userName = "${userName}";
        MQTT mqtt = new MQTT();
        mqtt.setHost(serverUrl);
        mqtt.setUserName(userName);
        mqtt.setPassword(jwtToken);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = {new Topic(topicName, QoS.AT_LEAST_ONCE)};
        connection.subscribe(topics);
        for (int i = 0; i < 5; i++) {
            String message = "Hello MQTT";
            connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        }
        connection.disconnect();
    }
}
