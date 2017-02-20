package guidance.utils;

import java.util.Hashtable;


public class HashUtils {

    /**
     * Method to create a hashtable from the header of particular files
     * 
     * @param line
     * @param separator
     * @return
     */
    public static Hashtable<String, Integer> createHashWithHeader(String line, String separator) {
        Hashtable<String, Integer> myHashLine = new Hashtable<String, Integer>();

        String[] splitted = line.split(separator);
        for (int i = 0; i < splitted.length; i++) {
            myHashLine.put(splitted[i], i);
        }
        return myHashLine;
    }

    /**
     * Method to create a hashtable from the header of particular files
     * 
     * @param line
     * @param separator
     * @return
     */
    public static Hashtable<Integer, String> createHashWithHeaderReversed(String line, String separator) {
        Hashtable<Integer, String> myHashLine = new Hashtable<Integer, String>();

        String[] splitted = line.split(separator);
        for (int i = 0; i < splitted.length; i++) {
            myHashLine.put(i, splitted[i]);
        }
        return myHashLine;
    }

}
