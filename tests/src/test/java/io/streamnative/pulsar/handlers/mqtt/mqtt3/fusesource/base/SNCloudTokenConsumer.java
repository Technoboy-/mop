package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.base;

import java.util.concurrent.TimeUnit;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

public class SNCloudTokenConsumer {

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
        boolean running = true;
        while (running) {
            Message record = connection.receive(1, TimeUnit.SECONDS);
            if (record == null) {
                break;
            }
            System.out.println("Receive record: topic : " + record.getTopic() + ", value : "
                    + new String(record.getPayload()));
        }
        connection.disconnect();
    }
}
