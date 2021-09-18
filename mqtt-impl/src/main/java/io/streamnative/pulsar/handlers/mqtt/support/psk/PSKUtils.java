package io.streamnative.pulsar.handlers.mqtt.support.psk;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

public class PSKUtils {

    public static List<PSKSecretKey> parse(File file) {
        List<PSKSecretKey> result = new LinkedList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine()) != null) {
                result.addAll(parse(line));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static List<PSKSecretKey> parse(String text) {
        List<PSKSecretKey> result = new LinkedList<>();
        Iterable<String> split = Splitter.on(";").split(text);
        split.forEach(line -> {
            List<String> keyIdentity = Splitter.on(":").limit(2).splitToList(line);
            String key = keyIdentity.get(0);
            String identity = keyIdentity.get(1);
            result.add(new PSKSecretKey(key, identity));
        });
        return result;
    }
}
