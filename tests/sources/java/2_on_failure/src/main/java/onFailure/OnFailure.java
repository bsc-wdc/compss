package onFailure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import es.bsc.compss.api.*;


public class OnFailure {

    private static final String FILE_NAME1 = "/tmp/sharedDisk/onFailure1.txt";
    private static final String FILE_NAME2 = "/tmp/sharedDisk/onFailure2.txt";
    private static final int M = 5; // number of tasks to be executed


    public static void main(String[] args){
        
     
        
        // Successors cancelation behavior
        System.out.println("Init on failure : CANCEL SUCCESSORS");
        try {
            onCancelation();
        } catch (numberException e){
            catchException (e);
        }
        
        //-------------------
        
        // Failure ignored behavior
        System.out.println("Init on failure : IGNORE FAILURE");
        try {
            onIgnoreFailure();
        } catch (numberException e){
            catchException (e);
        }
        
        //-------------------

        // Retry behavior
        System.out.println("Init on failure : RETRY");
        try {
            onRetry();
        } catch (numberException e){
            catchException (e);
        }
        
        //-------------------
        
        // Direct fail behavior
        System.out.println("Init on failure : DIRECT FAIL");
        try {
            onDirectFail();
        } catch (numberException e){
            catchException (e);
        }
    }
    
    private static void onRetry() throws numberException {
        initFiles();
        failingTaskRetry();
        COMPSs.barrier();
    }
    
    private static void onCancelation() throws numberException {
        initFiles();
        failingTaskCancelSuccessors();
        COMPSs.barrier();
    }
    
    private static void onIgnoreFailure() throws numberException {
        initFiles();
        failingTaskIgnoreFailure();
        COMPSs.barrier();
    }
    
    private static void onDirectFail() throws numberException {
        initFiles();
        failingTaskDirectFail();
        COMPSs.barrier();
    }

    private static void initFiles() {
        // Initialize two tests files
        try {
            newFile(FILE_NAME1);
            newFile(FILE_NAME2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static void failingTaskRetry() throws numberException {
        writeFile();
      //Process file contents
        int i=0;
        for (i=0;i<M;i++) {
//            //Depending on M, the executed task is different
//            if (i % 2 == 0) {
//                    OnFailureImpl.processParam2(FILE_NAME1);
//                    System.out.println("FILE 1");
//            } else {
                    OnFailureImpl.processParamRetry(FILE_NAME1);
//            }
//            System.out.println("Round:  " + i );
        }
    }
    
    private static void failingTaskCancelSuccessors() throws numberException {
        writeFile();
      //Process file contents
        int i=0;
        for (i=0;i<M;i++) {
//            //Depending on M, the executed task is different
//            if (i % 2 == 0) {
//                    OnFailureImpl.processParam2(FILE_NAME1);
//                    System.out.println("FILE 1");
//            } else {
                    OnFailureImpl.processParamCancelSuccessors(FILE_NAME1);
//            }
//            System.out.println("Round:  " + i );
        }
    }
    

    private static void failingTaskIgnoreFailure() throws numberException {
        writeFile();
      //Process file contents
        int i=0;
        for (i=0;i<M;i++) {
//            //Depending on M, the executed task is different
//            if (i % 2 == 0) {
//                    OnFailureImpl.processParam2(FILE_NAME1);
//                    System.out.println("FILE 1");
//            } else {
                    OnFailureImpl.processParamIgnoreFailure(FILE_NAME1);
//            }
//            System.out.println("Round:  " + i );
        }
    }
    
    private static void failingTaskDirectFail() throws numberException {
        writeFile();
      //Process file contents
        int i=0;
        for (i=0;i<M;i++) {
//            //Depending on M, the executed task is different
//            if (i % 2 == 0) {
//                    OnFailureImpl.processParam2(FILE_NAME1);
//                    System.out.println("FILE 1");
//            } else {
                    OnFailureImpl.processParamDirectFail(FILE_NAME1);
//            }
//            System.out.println("Round:  " + i );
        }
    }
    
    private static void writeFile() throws numberException {
        //Create and write random number to file
//        Random rand = new Random();
        
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(FILE_NAME1, false));
            writer.write(String.valueOf(1));
            writer.close();
//            writer = new BufferedWriter(new FileWriter(FILE_NAME2, false));
//            writer.write(String.valueOf(rand.nextInt(5)));
//            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
    
      private static void catchException(numberException e){
      System.out.println("exception caught!");
    }

}
