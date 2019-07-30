
package workerinmaster;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;


public class Tasks {

    /**
     * Prints into a file the values received as a parameter.
     *
     * @param file filepath where to print all the values
     * @param b boolean value to be printed on the file
     * @param c char value to be printed on the file
     * @param s string value to be printed on the file
     * @param by byte value to be printed on the file
     * @param sh short value to be printed on the file
     * @param i integer value to be printed on the file
     * @param l long value to be printed on the file
     * @param f float value to be printed on the file
     * @param d double value to be printed on the file
     */
    public static void testBasicTypes(String file, boolean b, char c, String s, byte by, short sh, int i, long l,
        float f, double d) {
        try {
            PrintStream ps = new PrintStream(file);
            ps.println("TEST BASIC TYPES");
            ps.println("- boolean: " + b);
            ps.println("- char: " + c);
            ps.println("- String: " + s);
            ps.println("- byte: " + by);
            ps.println("- short: " + sh);
            ps.println("- int: " + i);
            ps.println("- long: " + l);
            ps.println("- float: " + f);
            ps.println("- double: " + d);
            ps.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Creates a new file with the content passed in as a parameter.
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
     * Verifies that the content of the file matches the one passed in as a parameter.
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
     * Verifies that the content of the file matches the one passed in as a parameter and updates it.
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
     * Verifies that the content of the StringWrapper matches the one passed in as a parameter adn updates it.
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
     * Dummy task doing nothing.
     *
     * @return report of the execution
     * @throws java.lang.Exception Interrupted while sleeping
     */
    public static Report sleepTask() throws Exception {
        Report r = new Report();
        Thread.sleep(500);
        r.completedExecution();
        return r;
    }

}
