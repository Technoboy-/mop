package io.streamnative.pulsar.handlers.mqtt.utils;

import org.conscrypt.OpenSSLProvider;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public class Aes {

    private static final String CBC_PKCS5_PADDING = "AES/CBC/PKCS5Padding";//AES是加密方式 CBC是工作模式 PKCS5Padding是填充模式
    private static final String AES = "AES";//AES 加密
    private static final String SHA1PRNG = "SHA1PRNG";// SHA1PRNG 强随机种子算法, 要区别4.2以上版本的调用方法

    /**
     * 随机生成密钥，传同一个字符串，每次都生成的不一样
     * @param seed 一般传用户的密码
     * @return 返回密钥的byte数组
     * @throws Exception 异常
     */
    public static byte[] getRawKey(String seed) throws Exception {
        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
        sr.setSeed(seed.getBytes());  // 设置一个种子，这个种子一般是用户设定的密码。也可以是其它某个固定的字符串
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");  // 获得一个key生成器（AES加密模式）
        //AES中128位密钥版本有10个加密循环，192比特密钥版本有12个加密循环，256比特密钥版本则有14个加密循环。
        keyGen.init(128, sr);      // 设置密匙长度128位
        SecretKey key = keyGen.generateKey();  // 获得密匙
        return key.getEncoded();
    }

    private static byte[] getRawKey2(String seed) throws Exception {
//        SecureRandom sr = SecureRandom.getInstance("HmacSHA1");
//        sr.setSeed(seed.getBytes());  // 设置一个种子，这个种子一般是用户设定的密码。也可以是其它某个固定的字符串
        KeyGenerator keyGen = KeyGenerator.getInstance("HmacSHA1", new OpenSSLProvider());
        //AES中128位密钥版本有10个加密循环，192比特密钥版本有12个加密循环，256比特密钥版本则有14个加密循环。
        keyGen.init(128);      // 设置密匙长度128位
        SecretKey key = keyGen.generateKey();  // 获得密匙
        return key.getEncoded();
    }


    /**
     * 加密过程
     * @param raw 密钥的数组
     * @param clear 需要加密的byte数组
     * @return 加密后的byte数组
     * @throws Exception 异常
     */
    public static byte[] encrypt(byte[] raw, byte[] clear) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(raw, AES);
        Cipher cipher = Cipher.getInstance(CBC_PKCS5_PADDING);
        //iv偏移量传默认的16个0的字节数组
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(new byte[cipher.getBlockSize()]));
        return cipher.doFinal(clear);
    }


    /*
     * 解密
     */
    public static byte[] decrypt(byte[] raw, byte[] encrypted) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(raw, AES);
        Cipher cipher = Cipher.getInstance(CBC_PKCS5_PADDING);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(new byte[cipher.getBlockSize()]));
        return cipher.doFinal(encrypted);
    }

    public static void main(String[] args) throws Exception {
        byte[] rawKey = getRawKey("123");

        byte[] encrypt = encrypt(rawKey, "test".getBytes(StandardCharsets.UTF_8));
        byte[] decrypt = decrypt(rawKey, encrypt);
        System.out.println(new String(decrypt));
    }
}
