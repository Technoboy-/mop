package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SNCloudTokenProducerLocalTest {

    public static void main(String[] args) throws Exception {
        final String topicName = "xyz";
        Mqtt5SimpleAuth simpleAuth = Mqtt5SimpleAuth.builder().username("test-user")
                .password("eyJhbGciOiJSUzI1NiIsImtpZCI6IjI2NTNiZGFkLTI1M2UtNTc2MS05ZjU1LTdmYzVmZWViZDliZSIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsidXJuOnNuOnB1bHNhcjpvLXI1eHF2OnRlc3QtbW9wIl0sImV4cCI6MTcwOTI2NDgxMCwiaHR0cHM6Ly9zdHJlYW1uYXRpdmUuaW8vdXNlcm5hbWUiOiJ0ZXN0LW1vcC1wd2RAby1yNXhxdi5hdXRoLnRlc3QuY2xvdWQuZ2NwLnN0cmVhbW5hdGl2ZS5kZXYiLCJpYXQiOjE3MDY2NzI4MTMsImlzcyI6Imh0dHBzOi8vcGMtNTUzZWVhYjYudXNjZTEtd2hhbGUudGVzdC5nLnNuMi5kZXYvYXBpa2V5cy8iLCJqdGkiOiIyMzVlZmQyNDM1Yjg0Njg1YWFhMThiNzNlYjdhNGU4NyIsInBlcm1pc3Npb25zIjpbImFkbWluIiwiYWNjZXNzIl0sInNjb3BlIjoiYWRtaW4gYWNjZXNzIiwic3ViIjoiQWRWSTNJWldVRmROMlVWWUtSanVablBLakpiTHoyTnZAY2xpZW50cyJ9.PYBM8nO0HF-7Og3eSXPXIpZF7Sk83gmjXeYDibBYMSv5hbLyiuwlWGuY-Um0t6xiovhMkAv0EG0gNSQEY3YztJpDG1U8Zno8_poGr3Zg1fAWDJTQCL8HeUvyUZWEsS7HpQCCMetGEeTPGmXNsJawxwwUqmhgDaSLtxPxYJYY9BBsjBbZYRy_CTp40R46yFX7fiHb_oDrPiV97bsn6-Dy8N8Fy5H1VwYJDkbypWzBXdRDmJpXVTM9FJSy0AKLLyKY-pF-mpLfxiPa-uQYNbH3l4qt38G3eZQFjazfoLPXTP6iX1NigcEFOU8GOpZ6fyBbF6oKUYl57qREAvN-dGzaHg".getBytes())
                .build();
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(5682)
                .simpleAuth(simpleAuth)
                .buildBlocking();;
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
