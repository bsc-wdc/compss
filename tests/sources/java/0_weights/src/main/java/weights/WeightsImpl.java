package weights;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class WeightsImpl {

    public static String FILENAME = "file_out.txt";


    public static void genTask1(String filename1, String filename2, String filename3, String content) throws Exception {
        writeFile(filename1, content);
        writeFile(filename2, content);
        writeFile(filename3, content);
    }

    public static void genTask2(String filename1, String filename2, String filename3, String content) throws Exception {
        writeFile(filename1, content);
        writeFile(filename2, content);
        writeFile(filename3, content);
    }

    public static void readFiles1(String filename1, String filename2, String filename3) throws Exception {
        if (!filename1.startsWith("/tmp/COMPSsWorker01")) {
            throw new Exception("Filename is incorrect");
        }
    }

    public static void readFiles2(String filename1, String filename2, String filename3) throws Exception {
        if (!filename1.startsWith("/tmp/COMPSsWorker02")) {
            throw new Exception("Filename is incorrect");
        }
    }

    private static void writeFile(String filename, String content) {
        // Write first number to file
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(filename, false));
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
