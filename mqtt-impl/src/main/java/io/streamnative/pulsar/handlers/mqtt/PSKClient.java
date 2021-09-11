/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.streamnative.pulsar.handlers.mqtt.utils.Aes;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.commons.codec.binary.BinaryCodec;
import org.conscrypt.OpenSSLProvider;
import org.conscrypt.PSKKeyManager;
import org.jose4j.keys.HmacKey;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.pulsar.client.impl.PulsarChannelInitializer.TLS_HANDLER;

/**
 * PSK client.
 */
@Slf4j
public class PSKClient extends ChannelInitializer<SocketChannel> {

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(TLS_HANDLER, new SslHandler(createSSLEngine(ch)));
        ch.pipeline().addLast("decoder", new MqttDecoder());
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler", new PSKInboundHandler());
    }

    class PSKInboundHandler extends ChannelInboundHandlerAdapter{

        public void channelActive(ChannelHandlerContext ctx) {
            log.info("channelActive id : {}", ctx.channel().id());
        }
    }

    private SSLEngine createSSLEngine(SocketChannel ch) throws Exception{

//        addProvider();

//        KeyStore keyStore = KeyStore.getInstance("IAIKKeyStore");
//        FileInputStream fis = new FileInputStream("/Users/tboy/tboy/workspace/mop/mqtt-impl/src/main/resources/isasilk.keystore");
//        keyStore.load(fis, "password".toCharArray());
//
//        fis.close();
//
//        SecretKey secretKey = (SecretKey)keyStore.getKey("localhost", "password".toCharArray());
//        PSKCredential credential = new PSKCredential("localhost", new PreSharedKey(secretKey.getEncoded()));

        Provider provider = new OpenSSLProvider();
//
//        KeyStore keyStore = KeyStore.getInstance("jks", provider);
//
//        SecretKeySpec var27 = new SecretKeySpec(var12, 0, 16, "AES");
//        IvParameterSpec var13 = new IvParameterSpec(var12, 16, 16);
//
//        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", provider);
//        var10.init("AES/CBC/PKCS5Padding", 2, var27, var13, var5);
//
//        CipherInputStream var14 = new CipherInputStream((InputStream)var3, var10);

        PSKKeyManager myPskKeyManager = new PSKKeyManager() {
            @Override
            public String chooseServerKeyIdentityHint(Socket socket) {
                return "alpha";
            }

            @Override
            public String chooseServerKeyIdentityHint(SSLEngine engine) {
                return "alpha";
            }

            @Override
            public String chooseClientKeyIdentity(String identityHint, Socket socket) {
                return "lbstest";
            }

            @Override
            public String chooseClientKeyIdentity(String identityHint, SSLEngine engine) {
                log.info("chooseClientKeyIdentity2 identityHint :{}", identityHint);
                return "lbstest";
            }

            @Override
            public SecretKey getKey(String identityHint, String identity, Socket socket) {
                log.info("getKey identityHint1 :{}, identity :{}", identityHint, identity);
                return new SecretKey() {
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
                        return BinaryCodec.fromAscii("ruckus123!".toCharArray());
                    }
                };
            }

            @Override
            public SecretKey getKey(String identityHint, String identity, SSLEngine engine) {
                log.info("getKey identityHint2 :{}, identity :{}", identityHint, identity);
                return new SecretKey() {
                    @Override
                    public String getAlgorithm() {
                        return null;
                    }

                    @Override
                    public String getFormat() {
                        return null;
                    }

                    @Override
                    public byte[] getEncoded() {
                        try {
                            return "ruckus123!".getBytes(StandardCharsets.UTF_8);
//                            return "7275636b757331323321".getBytes(StandardCharsets.UTF_8);
//                            return Aes.getRawKey("7275636b757331323321");
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                };
//                return new HmacKey("7275636b757331323321".getBytes(StandardCharsets.UTF_8));
            }
        };

        List<String> protocol = new ArrayList<>();
        protocol.add("TLSv1.3");
        protocol.add("TLSv1.2");
        protocol.add("TLSv1.1");
        protocol.add("TLSv1");
        protocol.add("SSLv3");
        protocol.add("SSLv2");
        protocol.add("SSLv1");
//        protocol.add("SSLV3");
        List<String> ciphers = new ArrayList<>();
        ciphers.add("PSK-AES128-CBC-SHA");

        ApplicationProtocolConfig protocolConfig = new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.FATAL_ALERT,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.FATAL_ALERT,
                protocol);

        SslContext sslContext = SslContextBuilder.forClient()
                .startTls(true)
                .keyManager(myPskKeyManager)
                .sslProvider(SslProvider.JDK)
                .sslContextProvider(provider)
                .applicationProtocolConfig(protocolConfig)
                .protocols("TLSv1.2", "TLSv1.1", "TLSv1", "SSLv3")
//                .protocols("SSLV3")
                .ciphers(ciphers)
                .build();
        SSLEngine sslEngine = sslContext.newEngine(ch.alloc());
        sslEngine.setUseClientMode(true);
        return sslEngine;
    }

    public static void addProvider() {
        addProvider("iaik.security.provider.IAIK");
    }

    public static void addProvider(String var0) {
        try {
            Class var1 = Class.forName(var0);
            Provider var2 = (Provider)var1.newInstance();
            Security.insertProviderAt(var2, 1);
        } catch (ClassNotFoundException var3) {
            System.out.println("Provider IAIK not found. Add iaik_jce.jar or iaik_jce_full.jar to your classpath.");
            System.out.println("If you are going to use a different provider please take a look at Readme.html!");
            System.exit(0);
        } catch (Exception var4) {
            System.out.println("Error adding provider:");
            var4.printStackTrace(System.err);
            System.exit(0);
        }

    }

    public static void main(String[] args) throws Exception{
        Bootstrap client = new Bootstrap();

        EventLoopGroup group = new NioEventLoopGroup();
        client.group(group);
        client.channel(NioSocketChannel.class);
        client.handler(new PSKClient());
        CountDownLatch latch = new CountDownLatch(1);
        client.connect("localhost", 8883).addListener((ChannelFutureListener) future -> {
            latch.countDown();
            log.info("connected result : {}", future.isSuccess());
        });
        latch.await();
    }
}
