package testTaskGroups;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import es.bsc.compss.api.COMPSs;
import es.bsc.compss.api.COMPSsGroup;

public class TestTaskGroups {
    
    public static final int N = 3;
    public static final int M = 4;
    public static final int MAX_AVAILABLE = 1;

    public static final String FILE_NAME = "/tmp/sharedDisk/taskGroups.txt";

    public static void main(String[] args) throws Exception {
        newFile(FILE_NAME, true);
        
        System.out.println("[LOG] Test task groups");
        testTaskGroups();
        System.out.println("[LOG] Test task time out");
        testTaskTimeOut();
        COMPSs.getFile(FILE_NAME);
//             
//        // Shell commands to execute to check contents of file
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
//
//        // Read file content
//        readContents(process);

    }

    private static void testTaskGroups() throws Exception{
        try (COMPSsGroup group1 = new COMPSsGroup("BigGroup") ) {
            
            // Launch NUM_TASKS writing tasks
            for (int i=0; i<M; i++) {
                try (COMPSsGroup n = new COMPSsGroup("group"+i)){
                    for (int j=0; j<N; j++) {
                         TestTaskGroupsImpl.writeTwo(FILE_NAME);
                    }
                }
            }
        } 

        for (int i=0; i<N; i++) {
            COMPSs.barrierGroup("group"+i);
        }
            
        try (COMPSsGroup group = new COMPSsGroup("Group1")) {
            for (int i=0; i < M; i++) {
                TestTaskGroupsImpl.writeTwo(FILE_NAME);
            }
        }
    }
    
    private static void testTaskTimeOut() throws Exception {
        TestTaskGroupsImpl.timeOutTaskFast(FILE_NAME);
//        TestTaskGroupsImpl.timeOutTaskSlow(FILE_NAME);
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
//        if (out.toString() == "2222222222222222") {
//            throw new Exception("Incorrect number of writers " + out);
//        }
    }


}
