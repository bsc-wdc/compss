package es.bsc.compss.connectors.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class KeyManager {

    private static String keyPair = null;
    private static String keyType = null;
    private static String publicKey = null;
    private static String privateKey = null;


    public static String getKeyType() {
        if (keyType != null) {
            return keyType;
        }
        getKeyPair();
        return keyType;
    }

    public static String getKeyPair() {
        if (keyPair != null)
            return keyPair;
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
        keyType = "id_dsa";
        if (!keyf.exists() || !keyfpub.exists()) {
            keyfile = home + ".ssh" + fileSep + "id_rsa";
            keyf = new java.io.File(keyfile);
            keyfilePub = home + ".ssh" + fileSep + "id_rsa.pub";
            keyfpub = new java.io.File(keyfilePub);
            keyType = "id_rsa";
            if (!keyf.exists() || !keyfpub.exists()) {
                keyfile = home + ".ssh" + fileSep + "identity";
                keyf = new java.io.File(keyfile);
                keyfilePub = home + ".ssh" + fileSep + "identity.pub";
                keyfpub = new java.io.File(keyfilePub);
                keyType = "identity";
                if (!keyf.exists() || !keyfpub.exists()) {
                    keyfile = home + "ssh" + fileSep + "id_dsa";
                    keyf = new java.io.File(keyfile);
                    keyfilePub = home + "ssh" + fileSep + "id_dsa.pub";
                    keyfpub = new java.io.File(keyfilePub);
                    keyType = "id_dsa";
                    if (!keyf.exists() || !keyfpub.exists()) {
                        keyfile = home + "ssh" + fileSep + "id_rsa";
                        keyf = new java.io.File(keyfile);
                        keyfilePub = home + "ssh" + fileSep + "id_rsa.pub";
                        keyfpub = new java.io.File(keyfilePub);
                        keyType = "id_rsa";
                        if (!keyf.exists() || !keyfpub.exists()) {
                            keyfile = home + "ssh" + fileSep + "identity";
                            keyf = new java.io.File(keyfile);
                            keyfilePub = home + "ssh" + fileSep + "identity.pub";
                            keyfpub = new java.io.File(keyfilePub);
                            keyType = "identity";
                            if (!keyf.exists() || !keyfpub.exists()) {
                                return null;
                            }
                        }
                    }
                }
            }
        }
        keyPair = keyfile;
        return keyfile;
    }

    public static String getPublicKey(String keyfile) throws IOException {
        if (publicKey != null) {
            return publicKey;
        }

        BufferedReader input = new BufferedReader(new FileReader(keyfile + ".pub"));
        StringBuilder key = new StringBuilder();
        String sb = input.readLine();
        while (sb != null) {
            key.append(sb).append("\n");
            sb = input.readLine();
        }
        input.close();

        publicKey = key.toString();
        return publicKey;
    }

    public static String getPrivateKey(String keyfile) throws IOException {
        if (privateKey != null) {
            return privateKey;
        }

        BufferedReader input = new BufferedReader(new FileReader(keyfile));
        StringBuilder key = new StringBuilder();
        String sb = input.readLine();
        while (sb != null) {
            key.append(sb).append("\n");
            sb = input.readLine();
        }
        input.close();

        privateKey = key.toString();
        return privateKey;
    }

}
