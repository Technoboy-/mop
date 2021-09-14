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

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.tls.AlertLevel;
import org.bouncycastle.crypto.tls.PSKTlsClient;
import org.bouncycastle.crypto.tls.ServerOnlyTlsAuthentication;
import org.bouncycastle.crypto.tls.TlsAuthentication;
import org.bouncycastle.crypto.tls.TlsClientProtocol;
import org.bouncycastle.crypto.tls.TlsPSKIdentity;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.Security;
/**
 * PSK client.
 */
@Slf4j
public class BouncyCastleClient {

    public static void main(String[] args) throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        Z_PSKIdentity pskIdentity = new Z_PSKIdentity();

        Socket socket = new Socket(InetAddress.getByName("127.0.0.1"), 8883);

        SecureRandom secureRandom = new SecureRandom();
        TlsClientProtocol protocol = new TlsClientProtocol(socket.getInputStream(), socket.getOutputStream(),
                secureRandom);

        MyPSKTlsClient client = new MyPSKTlsClient(pskIdentity);
        protocol.connect(client);

        OutputStream output = protocol.getOutputStream();
        output.write("GET / HTTP/1.1\r\n\r\n".getBytes("UTF-8"));

        InputStream input = protocol.getInputStream();
        System.out.println(convertStreamToString(input));
    }

    static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    static class MyPSKTlsClient extends PSKTlsClient
    {

        public MyPSKTlsClient(TlsPSKIdentity id){
            super(id);
        }

        public void notifyAlertRaised(short alertLevel, short alertDescription, String message, Exception cause)
        {
            PrintStream out = (alertLevel == AlertLevel.fatal) ? System.err : System.out;
            out.println("TLS client raised alert (AlertLevel." + alertLevel + ", AlertDescription." + alertDescription + ")");
            if (message != null) {
                out.println(message);
            }
            if (cause != null) {
                cause.printStackTrace(out);
            }
        }

        public void notifyAlertReceived(short alertLevel, short alertDescription)
        {
            PrintStream out = (alertLevel == AlertLevel.fatal) ? System.err : System.out;
            out.println("TLS client received alert (AlertLevel." + alertLevel + ", AlertDescription."
                    + alertDescription + ")");
        }

        public TlsAuthentication getAuthentication()
                throws IOException
        {
            return new ServerOnlyTlsAuthentication()
            {
                public void notifyServerCertificate(org.bouncycastle.crypto.tls.Certificate serverCertificate)
                        throws IOException
                {
                    System.out.println("in getAuthentication");
                }
            };
        }
    }

    static class Z_PSKIdentity implements TlsPSKIdentity {

        void Z_PSKIdentity(){};

        public void skipIdentityHint(){
            System.out.println("skipIdentityHint called");
        }

        public void notifyIdentityHint(byte[] PSK_identity_hint){
            System.out.println("notifyIdentityHint called : " + new String(PSK_identity_hint));
        }

        public byte[] getPSKIdentity(){
            System.out.println("getPSKIdentity");
            return "lbstest".getBytes();
        }

        public byte[] getPSK(){
            System.out.println("getPSK");
//            return "7275636b757331323321".getBytes(StandardCharsets.UTF_8);
            return "ruckus123!".getBytes(StandardCharsets.UTF_8);
        }

    }
}
