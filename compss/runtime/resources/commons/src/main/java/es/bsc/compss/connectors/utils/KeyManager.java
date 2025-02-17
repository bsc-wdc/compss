/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.connectors.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class KeyManager {

    private static String KEY_PAIR = null;
    private static String KEY_TYPE = null;
    private static String PUBLIC_KEY = null;
    private static String PRIVATE_KEY = null;


    /**
     * Returns the key type.
     * 
     * @return The key type.
     */
    public static String getKeyType() {
        if (KEY_TYPE != null) {
            return KEY_TYPE;
        }
        getKeyPair();
        return KEY_TYPE;
    }

    /**
     * Returns the key pair.
     * 
     * @return The key pair.
     */
    public static String getKeyPair() {
        if (KEY_PAIR != null) {
            return KEY_PAIR;
        }
        String keyfile = null;
        String keyfilePub = null;
        String home = System.getProperty("user.home");
        String fileSep = System.getProperty("file.separator");

        if (home == null) {
            home = "";
        } else {
            home += fileSep;
        }

        keyfile = home + ".ssh" + fileSep + "id_dsa";
        java.io.File keyf = new java.io.File(keyfile);
        keyfilePub = home + ".ssh" + fileSep + "id_dsa.pub";
        java.io.File keyfpub = new java.io.File(keyfilePub);
        KEY_TYPE = "id_dsa";
        if (!keyf.exists() || !keyfpub.exists()) {
            keyfile = home + ".ssh" + fileSep + "id_rsa";
            keyf = new java.io.File(keyfile);
            keyfilePub = home + ".ssh" + fileSep + "id_rsa.pub";
            keyfpub = new java.io.File(keyfilePub);
            KEY_TYPE = "id_rsa";
            if (!keyf.exists() || !keyfpub.exists()) {
                keyfile = home + ".ssh" + fileSep + "identity";
                keyf = new java.io.File(keyfile);
                keyfilePub = home + ".ssh" + fileSep + "identity.pub";
                keyfpub = new java.io.File(keyfilePub);
                KEY_TYPE = "identity";
                if (!keyf.exists() || !keyfpub.exists()) {
                    keyfile = home + "ssh" + fileSep + "id_dsa";
                    keyf = new java.io.File(keyfile);
                    keyfilePub = home + "ssh" + fileSep + "id_dsa.pub";
                    keyfpub = new java.io.File(keyfilePub);
                    KEY_TYPE = "id_dsa";
                    if (!keyf.exists() || !keyfpub.exists()) {
                        keyfile = home + "ssh" + fileSep + "id_rsa";
                        keyf = new java.io.File(keyfile);
                        keyfilePub = home + "ssh" + fileSep + "id_rsa.pub";
                        keyfpub = new java.io.File(keyfilePub);
                        KEY_TYPE = "id_rsa";
                        if (!keyf.exists() || !keyfpub.exists()) {
                            keyfile = home + "ssh" + fileSep + "identity";
                            keyf = new java.io.File(keyfile);
                            keyfilePub = home + "ssh" + fileSep + "identity.pub";
                            keyfpub = new java.io.File(keyfilePub);
                            KEY_TYPE = "identity";
                            if (!keyf.exists() || !keyfpub.exists()) {
                                return null;
                            }
                        }
                    }
                }
            }
        }
        KEY_PAIR = keyfile;
        return keyfile;
    }

    /**
     * Returns the public key associated with the given key file.
     * 
     * @param keyfile Key file.
     * @return The public key.
     * @throws IOException When an error occurs processing the given key file.
     */
    public static String getPublicKey(String keyfile) throws IOException {
        if (PUBLIC_KEY != null) {
            return PUBLIC_KEY;
        }

        BufferedReader input = new BufferedReader(new FileReader(keyfile + ".pub"));
        StringBuilder key = new StringBuilder();
        String sb = input.readLine();
        while (sb != null) {
            key.append(sb).append("\n");
            sb = input.readLine();
        }
        input.close();

        PUBLIC_KEY = key.toString();
        return PUBLIC_KEY;
    }

    /**
     * Returns the private key associated with the given key file.
     * 
     * @param keyfile Key file.
     * @return The private key.
     * @throws IOException When an error occurs processing the given key file.
     */
    public static String getPrivateKey(String keyfile) throws IOException {
        if (PRIVATE_KEY != null) {
            return PRIVATE_KEY;
        }

        BufferedReader input = new BufferedReader(new FileReader(keyfile));
        StringBuilder key = new StringBuilder();
        String sb = input.readLine();
        while (sb != null) {
            key.append(sb).append("\n");
            sb = input.readLine();
        }
        input.close();

        PRIVATE_KEY = key.toString();
        return PRIVATE_KEY;
    }

}
