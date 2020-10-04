package fileStreamTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import es.bsc.distrostreamlib.api.files.FileDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;


public class Tasks {

    public static void writeFiles(FileDistroStream fds, int sleepTime) {
        // Create several new files and add them to the stream when written
        writeFilesToPath(Main.TEST_PATH, sleepTime);

        // Send end event when finished
        fds.close();
    }

    public static void writeFilesAlias(FileDistroStream fds, int sleepTime) {
        // Create several new files and add them to the stream when written
        writeFilesToPath(Main.TEST_PATH_ALIAS, sleepTime);

        // Send end event when finished
        fds.close();
    }

    public static void writeFilesToPath(String basePath, int sleepTime) {
        // Create several new files and add them to the stream when written
        for (int i = 0; i < Main.NUM_FILES; ++i) {
            // Sleep some time to delay production
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // File name
            String fileName = basePath + File.separator + Main.BASE_FILENAME + UUID.randomUUID();
            // Add content
            try (PrintWriter writer = new PrintWriter(fileName)) {
                System.out.println("WRITING FILE: " + fileName);
                writer.write("Test " + String.valueOf(i));
            } catch (FileNotFoundException fnfe) {
                System.err.println("Cannot write file " + fileName);
                fnfe.printStackTrace();
            }
            // Publish to stream is done automatically
        }
        // Sleep some time to delay production
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static Integer readFiles(FileDistroStream fds, int sleepTime) throws IOException, BackendException {
        // Process events until stream is closed
        Integer totalFiles = 0;
        while (!fds.isClosed()) {
            // Sleep between polls
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            Integer numNewFiles = pollNewFiles(fds);
            totalFiles = totalFiles + numNewFiles;

        }
        // Sleep between polls
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Although the stream is closed, there can still be pending events to process
        Integer numNewFiles = pollNewFiles(fds);
        totalFiles = totalFiles + numNewFiles;

        return totalFiles;
    }

    private static Integer pollNewFiles(FileDistroStream fds) throws IOException, BackendException {
        // Poll new files
        List<String> newFiles = fds.poll();
        // Process their content
        for (String fileName : newFiles) {
            System.out.println("RECEIVED FILE: " + fileName);
            String content = new String(Files.readAllBytes(Paths.get(fileName)));
            System.out.println(content);
        }
        // Increase processed files
        return newFiles.size();
    }

    public static Integer processFile(String fileName) throws IOException {
        System.out.println("RECEIVED FILE: " + fileName);
        String content = new String(Files.readAllBytes(Paths.get(fileName)));
        System.out.println(content);

        return 1;
    }
}
