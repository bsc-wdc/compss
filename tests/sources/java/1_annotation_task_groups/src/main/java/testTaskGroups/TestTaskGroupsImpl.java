package testTaskGroups;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Thread;

import es.bsc.compss.worker.COMPSsWorker;

public class TestTaskGroupsImpl {

    public static void writeTwo(String fileName) {
        writeFile(fileName, String.valueOf(2));
        System.out.println("2 written");
    }
    
    public static void timeOutTaskFast(String filename) throws Exception {
        String contents = "";
        try {
            Thread.sleep(1000);
            contents = readFile(filename);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        System.out.println("File contents : " + contents);
        System.out.println("Before cancellation point");
        COMPSsWorker.cancellationPoint();
        System.out.println("After the cancellation point");
    }
    
    public static void timeOutTaskSlow(String filename) throws Exception {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        System.out.println("Before cancellation point");
        COMPSsWorker.cancellationPoint();
        System.out.println("After the cancellation point");
    }
    
    public static void writeFile (String fileName, String i) {
        File f = new File(fileName);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(f, true));
            writer.write(i);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens
                writer.close();
            } catch (Exception e) {
            }
        }
    }
    
    public static String readFile (String fileName) {
        File f = new File(fileName);
        int res = 0;
        BufferedReader br = null;
        String contents = "";
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) { 
              contents = contents + line;
            }
                
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(res);
        return contents;
    }
}