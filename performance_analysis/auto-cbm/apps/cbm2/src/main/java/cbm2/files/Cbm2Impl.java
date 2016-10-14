package cbm2.files;

import java.lang.management.ManagementFactory;
import java.util.Random;


public class Cbm2Impl {

    public static void computeSleep(int time) {
        long t = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());
        while ((ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId()) - t) / 1000000 < time) {
            double x = new Random().nextDouble();
            for (int i = 0; i < 1000; ++i) {
                x = Math.atan(Math.sqrt(Math.pow(x, 10)));
            }
        }
    }

    public static void runTaskInOut(int sleepTime, String dummyFilePath) {
        /*
         * try //Para que sea equivalente a runTaskIn { //No podemos copiar un archivo a si mismo(no hace nada), lo
         * copiamos a un tmp Files.copy(Paths.get(dummyFilePath), Paths.get("dummyFile_tmp"),
         * StandardCopyOption.REPLACE_EXISTING); } catch (IOException e1) { e1.printStackTrace(); }
         */

        computeSleep(sleepTime);
    }

    public static void runTaskIn(int sleepTime, String dummyFilePath, String dummyFilePathOut) {
        computeSleep(sleepTime);
    }
}