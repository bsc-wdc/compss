package cache;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class CacheImpl {

    /*************************************************************************************************
     * SEND PARAMS METHODS
     *************************************************************************************************/
    public static Container method(int val, boolean bool, String s, String fileIn, String fileInOut, String fileOut, Container c1,
            Container c2) {

        // Print basic types
        System.out.println("INT VAL:    " + val);
        System.out.println("BOOL VAL:   " + bool);
        System.out.println("STRING VAL: " + s);

        // Write fileIn
        FileInputStream fis = null;
        FileOutputStream fos = null;
        try {
            fis = new FileInputStream(fileIn);
            int count = fis.read();
            fos = new FileOutputStream(fileIn);
            fos.write(++count); // Should be transparent to master since it is IN
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        // Write fileInOut
        fis = null;
        fos = null;
        try {
            fis = new FileInputStream(fileInOut);
            int count = fis.read();
            fos = new FileOutputStream(fileInOut);
            fos.write(++count); // Should be transparent to master since it is IN
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        // Write fileOut
        fos = null;
        int value = 10;
        try {
            fos = new FileOutputStream(fileOut);
            fos.write(value); // Should be transparent to master since it is IN
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        // Write IN container
        c1.setValue(c1.getValue() + 1);

        // Write INOUT container
        c2.setValue(c2.getValue() + 1);

        // Write OUT container
        Container c3 = new Container(value);

        return c3;
    }

    /*************************************************************************************************
     * OBJECT CHECKER METHODS
     *************************************************************************************************/
    public static void objectIN(Container c) {
        int val = c.getValue();
        c.setValue(val + 1); // Should be transparent to master since it is IN
    }

    public static void objectINOUT(Container c) {
        int val = c.getValue();
        c.setValue(val + 1);
    }

    public static Container objectOUT() {
        int val = 10;
        Container c = new Container(val);
        return c;
    }

    /*************************************************************************************************
     * FILE CHECKER METHODS
     *************************************************************************************************/
    public static void fileIN(String fileName) {
        FileInputStream fis = null;
        FileOutputStream fos = null;
        try {
            fis = new FileInputStream(fileName);
            int count = fis.read();
            fos = new FileOutputStream(fileName);
            fos.write(++count); // Should be transparent to master since it is IN
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    public static void fileINOUT(String fileName) {
        FileInputStream fis = null;
        FileOutputStream fos = null;
        try {
            fis = new FileInputStream(fileName);
            int count = fis.read();
            fos = new FileOutputStream(fileName);
            fos.write(++count);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    public static void fileOUT(String fileName) {
        int val = 10;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fileName);
            fos.write(val);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

}
