package onFailure;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;


public class OnFailureImpl {

    public static int processParam(String filename) throws numberException {
        File file = new File(filename); 
        BufferedReader br;
        String st="";
        try {
            
            //Read file contents
            br = new BufferedReader(new FileReader(file));
            st = br.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        //Content of file to integer
        int n = Integer.parseInt(st); 
        System.out.println("The number readed is : " + n);
        
        String str = String.valueOf(n+1);
        System.out.println("The number written is : " + str);
        BufferedWriter writer;
        try {
            //Write the new random integer to file
            new FileOutputStream(filename).close();
            writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(str);
            writer.close();
            System.out.println("Writing from a task from processParam1");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return n;
    }

    private static void failOnce(int n) throws numberException {
        //Exception thrown
        if (n==1) {
            throw new numberException ("The number is too low");
        }
    }
    
    private static void failMultiple(int n) throws numberException {
        //Exception thrown
        if (n < 3) {
            throw new numberException ("The number is too low");
        }
    }
    
    public static void processParamRetry(String filename) throws numberException {
        int n = processParam(filename);
        failMultiple(n);
    }
    
    public static void processParamCancelSuccessors(String filename) throws numberException {
        int n = processParam(filename);
        failOnce(n);
    }
    
    public static void processParamIgnoreFailure(String filename) throws numberException {
        int n = processParam(filename);
        failOnce(n);
    }
    
    public static void processParamDirectFail(String filename) throws numberException {
        int n = processParam(filename);
        failMultiple(n);
    }

}
