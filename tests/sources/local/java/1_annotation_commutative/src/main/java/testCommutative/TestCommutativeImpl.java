package testCommutative;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Thread;


public class TestCommutativeImpl {

    private static final int TASK_SLEEP_TIME = 1_000; // ms
    private static final int OTHER_TASK_SLEEP = 500;


    public static void writeOne(String fileName) {
        writeFile(fileName, String.valueOf(1));
        System.out.println("1 written");
        try {
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void writeTwoSlow(String fileName) {
        writeFile(fileName, String.valueOf(2));
        System.out.println("2 written");
        try {
            Thread.sleep(TASK_SLEEP_TIME * 6);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void writeCommutative(String fileName, String fileName2, String fileName3) {
        int res1 = readFile(fileName);
        int res2 = readFile(fileName2);
        int res = res1 + res2;
        System.out.println("The computed result is " + res);
        String contents = String.valueOf(res) + "\n";
        writeFileCommutative(fileName3, contents);
        try {
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void addOneCommutative(String fileName3) {
        int res1 = readFile(fileName3);
        int res = res1 + 1;
        System.out.println("The computed result is " + res);
        writeFile(fileName3, String.valueOf(res));
        try {
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static int checkContents(String fileName) {
        int res = readFileCheckContents(fileName);
        System.out.println("The final result is " + res);
        try {
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        writeFile(fileName, String.valueOf(res));
        return res;
    }

    public static void accumulateCommutative(String fileName, String fileName2) {
        int res1 = readFile(fileName);
        int res2 = readFile(fileName2);
        int res = res1 + res2;
        System.out.println("The computed result is " + res);
        writeFile(fileName2, String.valueOf(res));
        try {
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void reduce_and_check_task(String fileName, Integer param) {
        int result = readFile(fileName);
        result = result + param;
        System.out.println(result);
        writeFile(fileName, String.valueOf(result));
        try {
            Thread.sleep(OTHER_TASK_SLEEP);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Total result " + result);
    }

    public static Integer task(int i) {
        try {
            Thread.sleep(OTHER_TASK_SLEEP);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return i + 1;

    }

    public static void writeFile(String fileName, String i) {
        File f = new File(fileName);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(f));
            writer.write(i);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens
                writer.close();
            } catch (Exception e) {
            }
        }
        try {
            Thread.sleep(OTHER_TASK_SLEEP);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void writeFileCommutative(String fileName, String i) {
        File f = new File(fileName);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(f, true));
            writer.write(i);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens
                writer.close();
            } catch (Exception e) {
            }
        }
        try {
            Thread.sleep(OTHER_TASK_SLEEP);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static int readFile(String fileName) {
        File f = new File(fileName);
        int res = 0;
        BufferedReader br = null;
        String contents = "";
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) {
                contents = contents + line;
            }

            res = Integer.valueOf(contents);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(OTHER_TASK_SLEEP);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(res);
        return res;
    }

    public static int readFileCheckContents(String fileName) {
        File f = new File(fileName);
        int res = 0;
        BufferedReader br = null;
        String contents = "";
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) {
                contents = contents + line;
            }
            if (contents.length() == 2) {
                System.out.println("Length of contents 2");

                res = Integer.valueOf(contents) / 10;
                res = res + Integer.valueOf(contents) - res * 10;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(OTHER_TASK_SLEEP);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(res);
        return res;
    }

}
