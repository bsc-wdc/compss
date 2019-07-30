
package workerinmaster;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;


public class Tasks {

    // ------------------------------------------------------
    // ------------------------------------------------------
    // ------------- Create file With Content ---------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    /**
     * Creates a new file with the content passed in as a parameter on any node.
     *
     * @param content Content of the new file.
     * @param fileName filepath of the created file
     * @throws Exception error during the creation/writting of the file
     */
    public static void createFileWithContent(String content, String fileName) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(fileName, false)) {
            String value = content + "\n";
            fos.write(value.getBytes());
            fos.close();
        }
    }

    /**
     * Creates a new file with the content passed in as a parameter on the master process.
     *
     * @param content Content of the new file.
     * @param fileName filepath of the created file
     * @throws Exception error during the creation/writting of the file
     */
    public static void createFileWithContentMaster(String content, String fileName) throws Exception {
        createFileWithContent(content, fileName);
    }

    /**
     * Creates a new file with the content passed in as a parameter on COMPSsWorker0X.
     *
     * @param content Content of the new file.
     * @param fileName filepath of the created file
     * @throws Exception error during the creation/writting of the file
     */
    public static void createFileWithContentWorker(String content, String fileName) throws Exception {
        createFileWithContent(content, fileName);
    }

    /**
     * Creates a new file with the content passed in as a parameter on COMPSsWorker01.
     *
     * @param content Content of the new file.
     * @param fileName filepath of the created file
     * @throws Exception error during the creation/writting of the file
     */
    public static void createFileWithContentWorker01(String content, String fileName) throws Exception {
        createFileWithContent(content, fileName);
    }

    /**
     * Creates a new file with the content passed in as a parameter on COMPSsWorker02.
     *
     * @param content Content of the new file.
     * @param fileName filepath of the created file
     * @throws Exception error during the creation/writting of the file
     */
    public static void createFileWithContentWorker02(String content, String fileName) throws Exception {
        createFileWithContent(content, fileName);
    }

    // ------------------------------------------------------
    // ------------------------------------------------------
    // ----------------- Check file Content ----------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    /**
     * Verifies that the content of the file matches the one passed in as a parameter on any node.
     *
     * @param content text expected to be found on the file
     * @param fileName path of the file to analyze
     * @throws Exception could not open the file or content does not match
     */
    public static void checkFileWithContent(String content, String fileName) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            if ((line = br.readLine()) != null) {
                if (line.compareTo(content) != 0) {
                    StringBuilder errorMsg = new StringBuilder("File content is not what it was expected.\n");
                    errorMsg.append("Expecting:\n");
                    errorMsg.append(content).append("\n");
                    errorMsg.append("Found:\n");
                    errorMsg.append(line).append("\n");
                    System.err.println(errorMsg.toString());
                    throw new Exception("File content is not what it was expected.");
                }
            }
        }
    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter on the master process.
     *
     * @param content text expected to be found on the file
     * @param fileName path of the file to analyze
     * @throws Exception could not open the file or content does not match
     */
    public static void checkFileWithContentMaster(String content, String fileName) throws Exception {
        checkFileWithContent(content, fileName);

    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter on COMPSsWorker0X.
     *
     * @param content text expected to be found on the file
     * @param fileName path of the file to analyze
     * @throws Exception could not open the file or content does not match
     */
    public static void checkFileWithContentWorker(String content, String fileName) throws Exception {
        checkFileWithContent(content, fileName);

    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter on COMPSsWorker01.
     *
     * @param content text expected to be found on the file
     * @param fileName path of the file to analyze
     * @throws Exception could not open the file or content does not match
     */
    public static void checkFileWithContentWorker01(String content, String fileName) throws Exception {
        checkFileWithContent(content, fileName);

    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter on COMPSsWorker02.
     *
     * @param content text expected to be found on the file
     * @param fileName path of the file to analyze
     * @throws Exception could not open the file or content does not match
     */
    public static void checkFileWithContentWorker02(String content, String fileName) throws Exception {
        checkFileWithContent(content, fileName);

    }

    // ------------------------------------------------------
    // ------------------------------------------------------
    // ---------- Check and update file content ------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    /**
     * Verifies that the content of the file matches the one passed in as a parameter and updates it on any node.
     *
     * @param content text expected to be found on the file
     * @param newContent new content of the file
     * @param fileName path of the file to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateFileWithContent(String content, String newContent, String fileName)
        throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            if ((line = br.readLine()) != null) {
                if (line.compareTo(content) != 0) {
                    StringBuilder errorMsg = new StringBuilder("File content is not what it was expected.\n");
                    errorMsg.append("Expecting:\n");
                    errorMsg.append(content).append("\n");
                    errorMsg.append("Found:\n");
                    errorMsg.append(line).append("\n");
                    System.err.println(errorMsg.toString());
                    throw new Exception("File content is not what it was expected.");
                }
            }
            try (FileOutputStream fos = new FileOutputStream(fileName, false)) {
                String value = newContent + "\n";
                fos.write(value.getBytes());
                fos.close();
            }
        }
    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter and updates it on the master
     * process.
     *
     * @param content text expected to be found on the file
     * @param newContent new content of the file
     * @param fileName path of the file to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateFileWithContentMaster(String content, String newContent, String fileName)
        throws Exception {
        checkAndUpdateFileWithContent(content, newContent, fileName);
    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter and updates it on COMPSsWorker0X.
     *
     * @param content text expected to be found on the file
     * @param newContent new content of the file
     * @param fileName path of the file to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateFileWithContentWorker(String content, String newContent, String fileName)
        throws Exception {
        checkAndUpdateFileWithContent(content, newContent, fileName);
    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter and updates it on COMPSsWorker01.
     *
     * @param content text expected to be found on the file
     * @param newContent new content of the file
     * @param fileName path of the file to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateFileWithContentWorker01(String content, String newContent, String fileName)
        throws Exception {
        checkAndUpdateFileWithContent(content, newContent, fileName);
    }

    /**
     * Verifies that the content of the file matches the one passed in as a parameter and updates it on COMPSsWorker02.
     *
     * @param content text expected to be found on the file
     * @param newContent new content of the file
     * @param fileName path of the file to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateFileWithContentWorker02(String content, String newContent, String fileName)
        throws Exception {
        checkAndUpdateFileWithContent(content, newContent, fileName);
    }

    /**
     * Creates a new StringWrapper with the content passed in as a parameter.
     *
     * @param content Content of the new StringWrapper Object.
     * @return returns StringWrapper with the content passed in as parameter
     * @throws Exception error during the creation/writting of the StringWrapper
     */
    public static StringWrapper createObjectWithContent(String content) throws Exception {
        StringWrapper sw = new StringWrapper();
        sw.setValue(content);
        return sw;
    }

    /**
     * Creates a new StringWrapper with the content passed in as a parameter on master process.
     *
     * @param content Content of the new StringWrapper Object.
     * @return returns StringWrapper with the content passed in as parameter
     * @throws Exception error during the creation/writting of the StringWrapper
     */
    public static StringWrapper createObjectWithContentMaster(String content) throws Exception {
        return createObjectWithContent(content);
    }

    /**
     * Creates a new StringWrapper with the content passed in as a parameter on COMPSsWorker0X.
     *
     * @param content Content of the new StringWrapper Object.
     * @return returns StringWrapper with the content passed in as parameter
     * @throws Exception error during the creation/writting of the StringWrapper
     */
    public static StringWrapper createObjectWithContentWorker(String content) throws Exception {
        return createObjectWithContent(content);
    }

    /**
     * Creates a new StringWrapper with the content passed in as a parameter on COMPSsWorker01.
     *
     * @param content Content of the new StringWrapper Object.
     * @return returns StringWrapper with the content passed in as parameter
     * @throws Exception error during the creation/writting of the StringWrapper
     */
    public static StringWrapper createObjectWithContentWorker01(String content) throws Exception {
        return createObjectWithContent(content);
    }

    /**
     * Creates a new StringWrapper with the content passed in as a parameter on COMPSsWorker02.
     *
     * @param content Content of the new StringWrapper Object.
     * @return returns StringWrapper with the content passed in as parameter
     * @throws Exception error during the creation/writting of the StringWrapper
     */
    public static StringWrapper createObjectWithContentWorker02(String content) throws Exception {
        return createObjectWithContent(content);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter.
     *
     * @param content text expected to be found on the StringWrapper
     * @param sw StringWrapper to analyze
     * @throws Exception StringWrapper content does not match
     */
    public static void checkObjectWithContent(String content, StringWrapper sw) throws Exception {
        String line = sw.getValue();
        if (line.compareTo(content) != 0) {
            StringBuilder errorMsg = new StringBuilder("File content is not what it was expected.\n");
            errorMsg.append("Expecting:\n");
            errorMsg.append(content).append("\n");
            errorMsg.append("Found:\n");
            errorMsg.append(line).append("\n");
            System.err.println(errorMsg.toString());
            throw new Exception("File content is not what it was expected.");
        }
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter on master process.
     *
     * @param content text expected to be found on the StringWrapper
     * @param sw StringWrapper to analyze
     * @throws Exception StringWrapper content does not match
     */
    public static void checkObjectWithContentMaster(String content, StringWrapper sw) throws Exception {
        checkObjectWithContent(content, sw);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter on COMPSsWorker0X.
     *
     * @param content text expected to be found on the StringWrapper
     * @param sw StringWrapper to analyze
     * @throws Exception StringWrapper content does not match
     */
    public static void checkObjectWithContentWorker(String content, StringWrapper sw) throws Exception {
        checkObjectWithContent(content, sw);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter on COMPSsWorker01.
     *
     * @param content text expected to be found on the StringWrapper
     * @param sw StringWrapper to analyze
     * @throws Exception StringWrapper content does not match
     */
    public static void checkObjectWithContentWorker01(String content, StringWrapper sw) throws Exception {
        checkObjectWithContent(content, sw);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter on COMPSsWorker02.
     *
     * @param content text expected to be found on the StringWrapper
     * @param sw StringWrapper to analyze
     * @throws Exception StringWrapper content does not match
     */
    public static void checkObjectWithContentWorker02(String content, StringWrapper sw) throws Exception {
        checkObjectWithContent(content, sw);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter and updates it.
     *
     * @param content text expected to be found on the StringWrapper
     * @param newContent new content of the StringWrapper
     * @param sw StringWrapper to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateObjectWithContent(String content, String newContent, StringWrapper sw)
        throws Exception {

        String line = sw.getValue();

        if (line.compareTo(content) != 0) {
            StringBuilder errorMsg = new StringBuilder("File content is not what it was expected.\n");
            errorMsg.append("Expecting:\n");
            errorMsg.append(content).append("\n");
            errorMsg.append("Found:\n");
            errorMsg.append(line).append("\n");
            System.err.println(errorMsg.toString());
            throw new Exception("File content is not what it was expected.");
        }

        sw.setValue(newContent);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter and updates it on master
     * process.
     *
     * @param content text expected to be found on the StringWrapper
     * @param newContent new content of the StringWrapper
     * @param sw StringWrapper to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateObjectWithContentMaster(String content, String newContent, StringWrapper sw)
        throws Exception {
        checkAndUpdateObjectWithContent(content, newContent, sw);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter and updates it on
     * COMPSsWorker0X.
     *
     * @param content text expected to be found on the StringWrapper
     * @param newContent new content of the StringWrapper
     * @param sw StringWrapper to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateObjectWithContentWorker(String content, String newContent, StringWrapper sw)
        throws Exception {
        checkAndUpdateObjectWithContent(content, newContent, sw);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter and updates it on
     * COMPSsWorker01.
     *
     * @param content text expected to be found on the StringWrapper
     * @param newContent new content of the StringWrapper
     * @param sw StringWrapper to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateObjectWithContentWorker01(String content, String newContent, StringWrapper sw)
        throws Exception {
        checkAndUpdateObjectWithContent(content, newContent, sw);
    }

    /**
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter and updates it on
     * COMPSsWorker02.
     *
     * @param content text expected to be found on the StringWrapper
     * @param newContent new content of the StringWrapper
     * @param sw StringWrapper to analyze and update
     * @throws Exception could not open the file or content does not match
     */
    public static void checkAndUpdateObjectWithContentWorker02(String content, String newContent, StringWrapper sw)
        throws Exception {
        checkAndUpdateObjectWithContent(content, newContent, sw);
    }

}
