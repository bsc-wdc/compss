package model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread;
import storage.StorageException;
import storage.StorageObject;
import storageManager.StorageManager;


public class MyFile extends StorageObject implements Serializable {

    /**
     * Serial ID for Objects outside the runtime
     */
    private static final long serialVersionUID = 3L;

    private static final String ERROR_PERSIST = "[ERROR] Cannot persist object";
    private static final int TASK_SLEEP_TIME = 0_500; // ms

    private String filePath;


    public MyFile() {
        // Only for serialization
    }

    public MyFile(String path) {
        super();
        this.filePath = path;
    }

    public void writeThree() {
        try (FileOutputStream fos = new FileOutputStream(this.filePath, true)) {
            fos.write(3);
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Manually persist object to storage
        try {
            StorageManager.persist(this);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

    public void writeFour() {
        try (FileOutputStream fos = new FileOutputStream(this.filePath, true)) {
            fos.write(4);
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Manually persist object to storage
        try {
            StorageManager.persist(this);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

    public int getCount(String path) {
        int lines = 0;
        try (FileInputStream fis = new FileInputStream(path)) {
            while (fis.read() > 0) {
                lines++;
            }
            System.out.println(lines);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }
}
