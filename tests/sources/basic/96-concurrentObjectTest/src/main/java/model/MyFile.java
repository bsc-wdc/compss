package model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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

    private String filePath;


    public MyFile() {
        
    }

    public MyFile(String path) {
        super();
        this.filePath = path;
    }
    
    public void writeThree() {
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(this.filePath, true);
            fos.write(1);
            fos.close();
            Thread.sleep(2000);
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
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(this.filePath, true);
            fos.write(2);
            fos.close();
            Thread.sleep(2000);
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
    
    public void deleteContents() {
        PrintWriter writer;
        try {
            writer = new PrintWriter(this.filePath);
            writer.print("");
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public int getCount(String path) {
        FileInputStream fis;
        int lines = 0;
        try {
            fis = new FileInputStream(path);
            while (fis.read()>0) lines++;
            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }
}
