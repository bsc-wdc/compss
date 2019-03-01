package cache;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class Cache {

    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 0) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: cache.Cache");
            System.exit(-1);
        }

        // Check the send parameters behaviour
        check_sendParams();

        // Check the object cache
        check_objectCache();

        // Check the file cache
        check_fileCache();
    }

    public static void check_sendParams() {
        System.out.println("Checking send params...");

        // Init variables in master
        String fileName1 = "fileSP_IN.txt";
        String fileName2 = "fileSP_INOUT.txt";
        String fileName3 = "fileSP_OUT.txt";
        writeValueFile(fileName1, 1);
        writeValueFile(fileName2, 1);
        Container c1 = new Container(1);
        Container c2 = new Container(1);

        // Make call to generic method
        Container c3 = CacheImpl.method(1, // Basic type
                false, // Basic type
                "hello", // String (IN)
                fileName1, // File IN
                fileName2, // File INOUT
                fileName3, // File OUT
                c1, // Object IN
                c2 // Object INOUT
        );
        // Return is Object OUT

        // Print result (and sync)
        int val1 = readValueFile(fileName1);
        int val2 = readValueFile(fileName2);
        int val3 = readValueFile(fileName3);
        System.out.println("- Value File IN: " + val1);
        System.out.println("- Value File INOUT: " + val2);
        System.out.println("- Value File OUT: " + val3);
        System.out.println("- Value Object IN: " + c1.getValue());
        System.out.println("- Value Object INOUT: " + c2.getValue());
        System.out.println("- Value Object OUT: " + c3.getValue());
    }

    public static void check_objectCache() {
        check_IN_objectCache();
        check_INOUT_objectCache();
        check_OUT_objectCache();
        check_MIXT_objectCache();
    }

    public static void check_fileCache() {
        check_IN_fileCache();
        check_INOUT_fileCache();
        check_OUT_fileCache();
        check_MIXT_fileCache();
    }

    /*************************************************************************************************
     * OBJECT CHECKER METHODS
     *************************************************************************************************/
    private static void check_IN_objectCache() {
        System.out.println("Checking IN object Cache...");
        // Create object in Master
        Container c = new Container(1);

        // Move object to worker
        CacheImpl.objectIN(c);

        // Reuse object
        CacheImpl.objectIN(c);

        // Object back to master (sync)
        System.out.println("FINAL IN CONTAINER VALUE = " + c.getValue());
    }

    private static void check_INOUT_objectCache() {
        System.out.println("Checking INOUT object Cache...");
        // Create object in Master
        Container c = new Container(1);

        // Move object to worker
        CacheImpl.objectINOUT(c);

        // Reuse object
        CacheImpl.objectINOUT(c);

        // Object back to master (sync)
        System.out.println("FINAL INOUT CONTAINER VALUE = " + c.getValue());
    }

    private static void check_OUT_objectCache() {
        System.out.println("Checking OUT object Cache...");
        // Create object in Master
        Container c = new Container(1);

        // Move object to worker
        c = CacheImpl.objectOUT();

        // Reuse object
        c = CacheImpl.objectOUT();

        // Object back to master (sync)
        System.out.println("FINAL OUT CONTAINER VALUE = " + c.getValue());
    }

    private static void check_MIXT_objectCache() {
        System.out.println("Checking MIXT object Cache...");
        // Create object in Master
        Container c = new Container(1);

        // Move object to worker (with no update)
        CacheImpl.objectIN(c);

        // Reuse object (with update)
        CacheImpl.objectINOUT(c);

        // Reuse object (with update)
        CacheImpl.objectINOUT(c);

        // Reuse object
        c = CacheImpl.objectOUT();

        // Object back to master (sync)
        System.out.println("FINAL MIXT CONTAINER VALUE = " + c.getValue());
    }

    /*************************************************************************************************
     * FILE CHECKER METHODS
     *************************************************************************************************/
    private static void check_IN_fileCache() {
        System.out.println("Checking IN file Cache...");
        // Create object in Master
        String fileName = "fileIN.txt";
        writeValueFile(fileName, 1);

        // Move object to worker
        CacheImpl.fileIN(fileName);

        // Reuse object
        CacheImpl.fileIN(fileName);

        // Object back to master (sync)
        int finalValue = readValueFile(fileName);
        System.out.println("FINAL IN FILE VALUE = " + finalValue);
    }

    private static void check_INOUT_fileCache() {
        System.out.println("Checking INOUT file Cache...");
        // Create object in Master
        String fileName = "fileINOUT.txt";
        writeValueFile(fileName, 1);

        // Move object to worker
        CacheImpl.fileINOUT(fileName);

        // Reuse object
        CacheImpl.fileINOUT(fileName);

        // Object back to master (sync)
        int finalValue = readValueFile(fileName);
        System.out.println("FINAL INOUT FILE VALUE = " + finalValue);
    }

    private static void check_OUT_fileCache() {
        System.out.println("Checking OUT file Cache...");
        // Create object in Master
        String fileName = "fileOUT.txt";

        // Move object to worker
        CacheImpl.fileOUT(fileName);

        // Reuse object
        CacheImpl.fileOUT(fileName);

        // Object back to master (sync)
        int finalValue = readValueFile(fileName);
        System.out.println("FINAL IN FILE VALUE = " + finalValue);
    }

    private static void check_MIXT_fileCache() {
        System.out.println("Checking MIXT file Cache...");
        // Create object in Master
        String fileName = "fileMIXT.txt";
        writeValueFile(fileName, 1);

        // Move object to worker (with no update)
        CacheImpl.fileIN(fileName);

        // Reuse object (with update)
        CacheImpl.fileINOUT(fileName);

        // Reuse object (with update)
        CacheImpl.fileINOUT(fileName);

        // Reuse object
        CacheImpl.fileOUT(fileName);

        // Object back to master (sync)
        int finalValue = readValueFile(fileName);
        System.out.println("FINAL MIXT FILE VALUE = " + finalValue);
    }

    /*************************************************************************************************
     * FILE HELPER METHODS
     *************************************************************************************************/
    private static void writeValueFile(String fileName, int value) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fileName);
            fos.write(value);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    private static int readValueFile(String fileName) {
        FileInputStream fis = null;
        int value = -1;
        try {
            fis = new FileInputStream(fileName);
            value = fis.read();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        return value;
    }

}
