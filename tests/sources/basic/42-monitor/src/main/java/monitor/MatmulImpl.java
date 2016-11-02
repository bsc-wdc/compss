package monitor;

import java.io.FileOutputStream;


public class MatmulImpl {
    
    private static final int SLEEP_MONITOR_CHECKS = 1_000; // ms

    public static void multiplyAccumulative(double[] a, double[] b, double[] c) {
        int M = (int) Math.sqrt(a.length);
        for (int i = 0; i < M; i++) {
            for (int j = 0; j < M; j++) {
                for (int k = 0; k < M; k++) {
                    c[i * M + j] += a[i * M + k] * b[k * M + j];
                }
            }
        }
        
        // Sleep for monitor checks
        try {
            Thread.sleep(SLEEP_MONITOR_CHECKS);
        } catch (InterruptedException e) {
            // NO need to handle such exception
        }
    }

    public static double[] initBlock(int size) {
        double[] block = new double[size * size];
        for (int k = 0; k < size * size; k++) {
            double value = (double) (Math.random() * 10.0);
            block[k] = value;
        }
        
        return block;
    }

    public static void printBlock(double[] block) {
        for (int k = 0; k < block.length; k++) {
            System.out.print(block[k] + " ");
        }
        System.out.println("");
    }

    public static void blockToDisk(double[] block, int i, int j, int M) {
        try {
            FileOutputStream fos = new FileOutputStream("C." + i + "." + j);

            for (int k1 = 0; k1 < M; k1++) {
                for (int k2 = 0; k2 < M; k2++) {
                    String str = new Double(block[k1 * M + k2]).toString() + " ";
                    fos.write(str.getBytes());
                }
                fos.write("\n".getBytes());
            }
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
