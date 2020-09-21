package deleteFileSharedDisk;

import java.io.File;
import java.io.IOException;


public class DeleteFileSharedDisk {

    private static final String FILE_NAME = "/tmp/sharedDisk/fileDelete.txt";
    private static final int M = 5; // number of tasks to be executed


    public static void main(String[] args) throws Exception {

        // Initialize test file
        try {
            newFile(FILE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Execute task loop
        for (int i = 0; i < M; i++) {
            DeleteFileSharedDiskImpl.readFromFile(FILE_NAME);
        }

        File f = new File(FILE_NAME);
        f.delete();

        System.out.println("The file has been deleted");

    }

    private static void newFile(String fileName) throws IOException, InterruptedException {
        File file = new File(fileName);

        System.out.println("The file exists? " + file.exists());
        // Delete previous occurrences of the file
        if (file.exists()) {
            System.out.println("THE FILE EXISTS");
            file.delete();
        }
        Thread.sleep(400);
        System.out.println("Creating blank file");
        file.createNewFile();
    }

}
