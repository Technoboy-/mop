package io.streamnative.pulsar.handlers.mqtt.support.psk;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PSKSecretKeyStore {

    private final ConcurrentMap<String, PSKSecretKey> secretKeyMap = new ConcurrentHashMap<>();

    public void addPSKSecretKey(PSKSecretKey secretKey) {
        secretKeyMap.put(secretKey.getIdentity(), secretKey);
    }

    public void addPSKSecretKey(String key, String identity) {
        addPSKSecretKey(new PSKSecretKey(key, identity));
    }

    public PSKSecretKey getPSKSecretKey(String key) {
        return secretKeyMap.get(key);
    }
}
