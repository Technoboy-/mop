package io.streamnative.pulsar.handlers.mqtt.support.systemtopic.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class ConnectionEvent {

    private String hostAndPort;
}
