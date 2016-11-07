package integratedtoolkit.util;

import integratedtoolkit.types.exceptions.NonInstantiableException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class KeyManager {

    private static final String HOME = System.getProperty("user.home") + File.separator;

    private static final String[] KEY_TYPES = new String[] { "id_dsa", "id_rsa", "identity" };
    private static final String[] KEY_FOLDERS = new String[] { HOME + ".ssh" + File.separator, HOME + "ssh" + File.separator };

    private static String keyPair = null;
    private static String keyType = null;
    private static String publicKey = null;
    private static String privateKey = null;


    private KeyManager() {
        throw new NonInstantiableException("KeyManager");
    }

    public static String getKeyType() {
        if (keyType != null) {
            return keyType;
        }
        getKeyPair();
        return keyType;
    }

    public static String getKeyPair() {
        if (keyPair != null) {
            return keyPair;
        }

        for (String folder : KEY_FOLDERS) {
            for (String type : KEY_TYPES) {
                String keyfile = folder + keyType;
                File pvKey = new File(keyfile);
                File pbKey = new File(keyfile + ".pub");
                if (pvKey.exists() && pbKey.exists()) {
                    keyType = type;
                    keyPair = keyfile;
                    return keyfile;
                }
            }
        }
        return null;
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
