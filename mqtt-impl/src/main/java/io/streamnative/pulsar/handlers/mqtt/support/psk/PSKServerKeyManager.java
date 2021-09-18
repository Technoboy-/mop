package io.streamnative.pulsar.handlers.mqtt.support.psk;

import org.conscrypt.PSKKeyManager;

import javax.crypto.SecretKey;
import javax.net.ssl.SSLEngine;
import java.net.Socket;

public class PSKServerKeyManager implements PSKKeyManager {

    private PSKConfiguration configuration;

    public PSKServerKeyManager(PSKConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public String chooseServerKeyIdentityHint(Socket socket) {
        return configuration.getIdentityHint();
    }

    @Override
    public String chooseServerKeyIdentityHint(SSLEngine engine) {
        return configuration.getIdentityHint();
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, Socket socket) {
        return null;
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, SSLEngine engine) {
        return null;
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, Socket socket) {
        return configuration.getKeyStore().getPSKSecretKey(identity);
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, SSLEngine engine) {
        return configuration.getKeyStore().getPSKSecretKey(identity);
    }
}
