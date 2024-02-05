package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.base;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;

public class SNCloudTokenProducerLocalTest {

    public static void main(String[] args) throws Exception{
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", 5682);
        mqtt.setUserName("superUser");
        final String jwtToken = "${token}";
        mqtt.setPassword("eyJhbGciOiJSUzI1NiIsImtpZCI6IjI2NTNiZGFkLTI1M2UtNTc2MS05ZjU1LTdmYzVmZWViZDliZSIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsidXJuOnNuOnB1bHNhcjpvLXI1eHF2OnRlc3QtbW9wIl0sImV4cCI6MTcwOTI2NDgxMCwiaHR0cHM6Ly9zdHJlYW1uYXRpdmUuaW8vdXNlcm5hbWUiOiJ0ZXN0LW1vcC1wd2RAby1yNXhxdi5hdXRoLnRlc3QuY2xvdWQuZ2NwLnN0cmVhbW5hdGl2ZS5kZXYiLCJpYXQiOjE3MDY2NzI4MTMsImlzcyI6Imh0dHBzOi8vcGMtNTUzZWVhYjYudXNjZTEtd2hhbGUudGVzdC5nLnNuMi5kZXYvYXBpa2V5cy8iLCJqdGkiOiIyMzVlZmQyNDM1Yjg0Njg1YWFhMThiNzNlYjdhNGU4NyIsInBlcm1pc3Npb25zIjpbImFkbWluIiwiYWNjZXNzIl0sInNjb3BlIjoiYWRtaW4gYWNjZXNzIiwic3ViIjoiQWRWSTNJWldVRmROMlVWWUtSanVablBLakpiTHoyTnZAY2xpZW50cyJ9.PYBM8nO0HF-7Og3eSXPXIpZF7Sk83gmjXeYDibBYMSv5hbLyiuwlWGuY-Um0t6xiovhMkAv0EG0gNSQEY3YztJpDG1U8Zno8_poGr3Zg1fAWDJTQCL8HeUvyUZWEsS7HpQCCMetGEeTPGmXNsJawxwwUqmhgDaSLtxPxYJYY9BBsjBbZYRy_CTp40R46yFX7fiHb_oDrPiV97bsn6-Dy8N8Fy5H1VwYJDkbypWzBXdRDmJpXVTM9FJSy0AKLLyKY-pF-mpLfxiPa-uQYNbH3l4qt38G3eZQFjazfoLPXTP6iX1NigcEFOU8GOpZ6fyBbF6oKUYl57qREAvN-dGzaHg");
        String topicName = "persistent://public/default/testAuthentication";
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = {new Topic(topicName, QoS.AT_LEAST_ONCE)};
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received = connection.receive();
        System.out.println("received : {}" + new String(received.getPayload()));
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }
}
