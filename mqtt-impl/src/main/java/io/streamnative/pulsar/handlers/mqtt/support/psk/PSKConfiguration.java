package io.streamnative.pulsar.handlers.mqtt.support.psk;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Getter
public class PSKConfiguration {

    static List<String> defaultApplicationProtocols = new ArrayList<>();

    static List<String> defaultCiphers = new ArrayList<>();

    static List<String> defaultProtocols = new ArrayList<>();

    static {
        defaultApplicationProtocols.add(ApplicationProtocolNames.HTTP_2);
        defaultApplicationProtocols.add(ApplicationProtocolNames.HTTP_1_1);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_1);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_2);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_3);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_3_1);
        //
        defaultCiphers.add("PSK-AES128-CBC-SHA");
        //
        defaultProtocols.add("TLSv1.2");
        defaultProtocols.add("TLSv1.1");
        defaultProtocols.add("TLSv1");
        defaultProtocols.add("SSLv3");

    }
    static ApplicationProtocolConfig defaultProtocolConfig = new ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            defaultApplicationProtocols);

    private PSKSecretKeyStore keyStore = new PSKSecretKeyStore();

    @Setter
    private String identityHint;

    private File identityFile;

    private String identityText;

    private List<String> applicationProtocols = defaultApplicationProtocols;

    private List<String> ciphers = defaultCiphers;

    private List<String> protocols = defaultProtocols;

    private ApplicationProtocolConfig protocolConfig = defaultProtocolConfig;

    @Setter
    private PSKSecretKey secretKey;

    public void setIdentityFile(File file) {
        this.identityFile = file;
        List<PSKSecretKey> pskKeys = PSKUtils.parse(file);
        pskKeys.forEach(item -> {
            item.setHint(identityHint);
            keyStore.addPSKSecretKey(item);
        });
    }

    public void setIdentityText(String identityText) {
        this.identityText = identityText;
        List<PSKSecretKey> pskKeys = PSKUtils.parse(identityText);
        pskKeys.forEach(item -> {
            item.setHint(identityHint);
            keyStore.addPSKSecretKey(item);
        });
    }

}
