package wallClockLimit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import es.bsc.compss.worker.COMPSsException;
import es.bsc.compss.worker.COMPSsWorker;


public class WallClockTestImpl {

    public static void inoutLongTask(String fileName) throws Exception {
        for (int j = 0; j <= 600; j++) {
            Thread.sleep(50);
            System.out.println(j);
            COMPSsWorker.cancellationPoint();
        }
    }

    public static void longTask(String fileName) throws Exception {
        for (int j = 0; j <= 600; j++) {
            Thread.sleep(50);
            System.out.println(j);
            COMPSsWorker.cancellationPoint();
        }
    }

    public static void executedTask(String fileName) {
        System.out.println("Filename: " + fileName);
        writeFile(fileName, String.valueOf(1));
        System.out.println("1 written");
    }

    public static void cancelledTask(String fileName) throws Exception {
        for (int j = 0; j <= 5; j++) {
            Thread.sleep(1000);
            System.out.println(j);
            COMPSsWorker.cancellationPoint();
        }
    }

    public static void writeFile(String fileName, String i) {
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

    public static String readFile(String fileName) {
        File f = new File(fileName);
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
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return contents;
    }
}