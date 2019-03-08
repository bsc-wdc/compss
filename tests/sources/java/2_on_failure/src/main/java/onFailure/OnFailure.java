package onFailure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class OnFailure {

    private static final String FILE_NAME1 = "/tmp/sharedDisk/onFailure1.txt";
    private static final String FILE_NAME2 = "/tmp/sharedDisk/onFailure2.txt";
    private static final int M = 5; // number of tasks to be executed


    public static void main(String[] args) throws numberException{
        
     // Initialize two tests files
        try {
            newFile(FILE_NAME1);
            newFile(FILE_NAME2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Create and write random number to file
        Random rand = new Random();
        
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(FILE_NAME1, false));
            writer.write(String.valueOf(rand.nextInt(5)));
            writer.close();
            writer = new BufferedWriter(new FileWriter(FILE_NAME2, false));
            writer.write(String.valueOf(rand.nextInt(5)));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        //Process file contents
        int i=0;
        for (i=0;i<M;i++) {
            //Depending on M, the executed task is different
            if (i % 2 == 0) {
                    OnFailureImpl.processParam2(FILE_NAME1);
                    System.out.println("FILE 1");
            } else {
                    OnFailureImpl.processParam(FILE_NAME2);
                    System.out.println("FILE 2");
            }
            System.out.println("Round:  " + i );
        }
        System.out.println("Param processed " );
    }
    
    //Function that creates a new empty file
    private static void newFile(String fileName) throws IOException {
        File file = new File(fileName);
        // Delete previous occurrences of the file
        if (file.exists()) {
            file.delete();
        }
        // Create the file and directories if required
        boolean createdFile = file.createNewFile();
        if (!createdFile) {
            throw new IOException("[ERROR] Cannot create test file");
        }
    }
    
//    private static void catchException(numberException e){
//        System.out.println("exception caught!");
//    }
}
