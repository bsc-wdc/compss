package onFailure;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;


public class OnFailureImpl {

    public static void processParam(String filename) throws numberException {
        Random rand = new Random();
        File file = new File(filename); 
        BufferedReader br;
        String st="";
        try {
            Thread.sleep(500);
            
            //Read file contents
            br = new BufferedReader(new FileReader(file));
            st = br.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        
        //Content of file to integer
        int n = Integer.parseInt(st); 
        System.out.println("The number readed is : " + n);
        
        //Exception thrown
        if (n>=0) {
            throw new numberException ("The number is too low");
        }
            
        String str = String.valueOf(rand.nextInt(5));
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
       
    }
    
    public static void processParam2(String filename) throws numberException {
        Random rand = new Random();
        File file = new File(filename); 
        BufferedReader br;
        String st="";
        try {
            Thread.sleep(500);
            
            //Read file contents
            br = new BufferedReader(new FileReader(file));
            st = br.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        
        //Content of file to integer
        int n = Integer.parseInt(st); 
        System.out.println("The number readed is : " + n);
      //Exception thrown
        if (n>=0) {
            throw new numberException ("The number is too low");
        }
        String str = String.valueOf(rand.nextInt(5));
        BufferedWriter writer;
        try {
            //Write the new random integer to file
            new FileOutputStream(filename).close();
            writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(str);
            writer.close();
            System.out.println("Writing from a task from processParam2");
        } catch (IOException e) {
            e.printStackTrace();
        }
       
    }

}
