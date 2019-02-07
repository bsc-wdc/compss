package model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;

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
    
    public void writeOne() {
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(this.filePath, true);
            fos.write(1);
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
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
