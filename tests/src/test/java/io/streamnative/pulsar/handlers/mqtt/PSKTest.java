package io.streamnative.pulsar.handlers.mqtt; /**
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
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKConfiguration;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKEngineFactory;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKSecretKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.BinaryCodec;
import org.conscrypt.OpenSSLProvider;
import org.conscrypt.PSKKeyManager;

import javax.crypto.SecretKey;
import javax.net.ssl.SSLEngine;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.client.impl.PulsarChannelInitializer.TLS_HANDLER;

/**
 * PSK client.
 */
@Slf4j
public class PSKTest extends ChannelInitializer<SocketChannel> {

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        PSKConfiguration pskConfiguration = new PSKConfiguration();
        PSKSecretKey pskSecretKey = new PSKSecretKey("lbstest", "ruckus123!");
        pskSecretKey.setHint("alpha");
        pskConfiguration.setSecretKey(pskSecretKey);
        ch.pipeline().addLast(TLS_HANDLER, new SslHandler(PSKEngineFactory.createClientEngine(ch, pskConfiguration)));
        ch.pipeline().addLast("decoder", new MqttDecoder());
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler", new PSKInboundHandler());
    }

    class PSKInboundHandler extends ChannelInboundHandlerAdapter{

        public void channelActive(ChannelHandlerContext ctx) {
            log.info("channelActive id : {}", ctx.channel().id());
        }
    }

    public static void main(String[] args) throws Exception{
        Bootstrap client = new Bootstrap();

        EventLoopGroup group = new NioEventLoopGroup();
        client.group(group);
        client.channel(NioSocketChannel.class);
        client.handler(new PSKTest());
        CountDownLatch latch = new CountDownLatch(1);
        client.connect("localhost", 8883).addListener((ChannelFutureListener) future -> {
            log.info("connected result : {}", future.isSuccess());
        });
        latch.await(10, TimeUnit.MINUTES);
    }
}
