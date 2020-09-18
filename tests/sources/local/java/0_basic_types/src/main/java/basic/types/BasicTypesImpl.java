package basic.types;

import java.io.FileNotFoundException;
import java.io.PrintStream;


public class BasicTypesImpl {

    /**
     * Tests the java basic types.
     * 
     * @param file File path.
     * @param b Boolean value.
     * @param c Char value.
     * @param s String value.
     * @param by Byte value.
     * @param sh Short value.
     * @param i Int value.
     * @param l Long value.
     * @param f Float value.
     * @param d Double value.
     * @throws FileNotFoundException When file is not found.
     */
    public static void testBasicTypes(String file, boolean b, char c, String s, byte by, short sh, int i, long l,
        float f, double d) throws FileNotFoundException {

        try (PrintStream ps = new PrintStream(file)) {
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
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            throw fnfe;
        }
    }

}