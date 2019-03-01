package basicTypes;

import java.io.FileNotFoundException;
import java.io.PrintStream;


public class BasicTypesImpl {

    public static void testBasicTypes(String file, boolean b, char c, String s, byte by, short sh, int i, long l, float f, double d) {
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

}