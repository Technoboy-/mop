package io.streamnative.pulsar.handlers.mqtt.support.psk;

import org.conscrypt.PSKKeyManager;

import javax.crypto.SecretKey;
import javax.net.ssl.SSLEngine;
import java.net.Socket;

public class PSKClientKeyManager implements PSKKeyManager {

    protected PSKSecretKey secretKey;

    public PSKClientKeyManager(PSKSecretKey pskSecretKey) {
        this.secretKey = pskSecretKey;
    }

    @Override
    public String chooseServerKeyIdentityHint(Socket socket) {
        return secretKey.getHint();
    }

    @Override
    public String chooseServerKeyIdentityHint(SSLEngine engine) {
        return secretKey.getHint();
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, Socket socket) {
        return secretKey.getIdentity();
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, SSLEngine engine) {
        return secretKey.getIdentity();
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, Socket socket) {
        return secretKey;
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, SSLEngine engine) {
        return secretKey;
    }
}
