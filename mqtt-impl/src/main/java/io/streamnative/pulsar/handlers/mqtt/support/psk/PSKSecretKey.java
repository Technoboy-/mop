package io.streamnative.pulsar.handlers.mqtt.support.psk;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor
public class PSKSecretKey implements SecretKey {

    @Getter
    @Setter
    private String hint;

    @Getter
    private final String identity;
    private final String secret;

    @Override
    public String getAlgorithm() {
        return "PSK";
    }

    @Override
    public String getFormat() {
        return "RAW";
    }

    @Override
    public byte[] getEncoded() {
        return secret.getBytes(StandardCharsets.UTF_8);
    }
}
