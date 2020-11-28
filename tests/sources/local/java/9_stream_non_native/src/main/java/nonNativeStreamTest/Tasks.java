package nonNativeStreamTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import es.bsc.distrostreamlib.api.files.FileDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;


public class Tasks {

    public static Integer readFiles(FileDistroStream fds, int sleepTime) throws IOException, BackendException {
        // Process events until stream is closed
        Integer totalFiles = 0;
        while (!fds.isClosed()) {
            Integer numNewFiles = pollNewFiles(fds);
            totalFiles = totalFiles + numNewFiles;

            // Sleep between polls
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
        int i = 0;
        for (String fileName : newFiles) {
            i++;
            System.out.println("RECEIVED FILE: " + fileName);
            String content = new String(Files.readAllBytes(Paths.get(fileName)));
            System.out.println(content);
        }
        // Increase processed files
        return i;
    }

}
