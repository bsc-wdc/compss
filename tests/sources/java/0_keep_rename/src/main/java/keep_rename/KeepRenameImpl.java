package keep_rename;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;


public class KeepRenameImpl {

    public static String FILENAME = "file_out.txt";


    public static void writeFileKeepRename(String file, String content) throws Exception {
        if (!file.endsWith(".IT")) {
            throw new Exception("File not renamed");
        } else {
            writeFile(file, content);
        }

    }

    public static void readFileNoRename(String file, String content) throws Exception {
        if (!file.endsWith(FILENAME)) {
            throw new Exception("Filename is incorrect");
        } else {
            if (!readFile(file).equals(content)) {
                throw new Exception("Incorrect content.");
            }
        }
    }

    public static void writeListKeepRename(List<String> files, String content) throws Exception {
        for (String file : files) {
            if (!file.endsWith(".IT")) {
                throw new Exception("File not renamed");
            } else {
                writeFile(file, content);
            }
        }
    }

    public static void readListNoRename(List<String> files, String content) throws Exception {
        for (String file : files) {
            readFileNoRename(file, content);
        }
    }

    private static String readFile(String filename) {
        File file = new File(filename);
        String st = "";
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            // Read file contents
            st = br.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return st;
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
