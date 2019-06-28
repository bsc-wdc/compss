package testTaskGroups;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import es.bsc.compss.api.COMPSs;
import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.worker.COMPSsException;

public class TestTaskGroups {
    
    public static final int N = 3;
    public static final int M = 4;
    public static final int MAX_AVAILABLE = 1;

    public static final String FILE_NAME = "/tmp/sharedDisk/taskGroups.txt";
    public static final String FILE_NAME2 = "/tmp/sharedDisk/taskGroups2.txt";
    
    public static void main(String[] args) throws Exception {
        newFile(FILE_NAME, true);
        newFile(FILE_NAME2, true);
        
        System.out.println("[LOG] Test task groups");
        testTaskGroups();
        
        System.out.println("[LOG] Test task group exeptions");
        testGroupExceptions();
        
        COMPSs.getFile(FILE_NAME);
        
        System.out.println("[LOG] Test task time out");
        testTaskTimeOut();
//        

        
//        System.out.println("[LOG] Test on failure ignore");
//        testIgnoreFailure();
        

        
        //Check file contents
        // Shell commands to execute to check contents of file
//        ArrayList<String> commands = new ArrayList<String>();
//        commands.add("/bin/cat");
//        commands.add(FILE_NAME);
//
//        // ProcessBuilder start
//        ProcessBuilder pb = new ProcessBuilder(commands);
//        pb.redirectErrorStream(true);
//        Process process = null;
//        try {
//            process = pb.start();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // Read file content
//        readContents(process);

    }
    
    private static void testIgnoreFailure() throws Exception {
        System.out.println("Executing write One ");
        TestTaskGroupsImpl.writeOnFailure(FILE_NAME);
        TestTaskGroupsImpl.writeFour(FILE_NAME);
    }

    private static void testGroupExceptions() throws InterruptedException {
        try (COMPSsGroup a = new COMPSsGroup("FailedGroup", true)){
            System.out.println("Executing write One ");
            for (int j=0; j<N; j++) {
                TestTaskGroupsImpl.writeOne(FILE_NAME);
           }
        }catch (COMPSsException e) {
            TestTaskGroupsImpl.writeThree(FILE_NAME);
            System.out.println("Exception caught!!");

        }catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            TestTaskGroupsImpl.writeFour(FILE_NAME);
        }
    }

    private static void testTaskGroups() throws Exception{
        // Check of nested groups
        try (COMPSsGroup group1 = new COMPSsGroup("BigGroup", true) ) {
            // Create several nested groups containing writing tasks
            for (int i=0; i<M; i++) {
                try (COMPSsGroup n = new COMPSsGroup("group"+i, true)){
                    for (int j=0; j<N; j++) {
                         TestTaskGroupsImpl.writeTwo(FILE_NAME);
                    }
                }
            }
        } 
//
//        // Perform a barrier for every created group
//        for (int i=0; i<N; i++) {
//            COMPSs.barrierGroup("group"+i);
//        }
        
        // Creation of individual group of M tasks
        try (COMPSsGroup group = new COMPSsGroup("Group1", true)) {
            for (int i=0; i < M; i++) {
                TestTaskGroupsImpl.writeTwo(FILE_NAME);
            }
        }
    }
    
    // Two tasks to check time out. The second takes more time than expected
    private static void testTaskTimeOut() throws Exception {
        TestTaskGroupsImpl.timeOutTaskFast(FILE_NAME);
        TestTaskGroupsImpl.timeOutTaskSlow(FILE_NAME);
    }
    

    private static void newFile(String fileName, boolean create) throws IOException {
        File file = new File(fileName);
        // Delete previous occurrences of the file
        if (file.exists()) {
            file.delete();
        }
        if (create) {
            // Create the file and directories if required
            boolean createdFile = file.createNewFile();
            if (!createdFile) {
                throw new IOException("[ERROR] Cannot create test file");
            }
        }

    }
    
    private static void readContents(Process process) throws Exception {
        // Read output of file
        StringBuilder out = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null, previous = null;
        int count = 0;
        try {
            while ((line = br.readLine()) != null)
                if (!line.equals(previous)) {
                    previous = line;
                    out.append(line).append('\n');
                    count = count + 1;
                }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Exception if number of writers has not been correct
        if (out.toString() == "2222222222222222") {
            throw new Exception("Incorrect number of writers " + out);
        }
    }


}
