package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Gap;
import integratedtoolkit.util.ResourceScheduler;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;


public class SchedulingOptimizer extends Thread {

    private static long OPTIMIZATION_THRESHOLD = 5_000;
    private DefaultScheduler scheduler;
    private boolean stop = false;
    private Semaphore sem = new Semaphore(0);

    public SchedulingOptimizer(DefaultScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void run() {
        long lastUpdate = System.currentTimeMillis();
        while (!stop) {
            long optimizationTS = System.currentTimeMillis();
            ResourceScheduler<?>[] workers = scheduler.getWorkers();
            OptimizedWorker[] optimizeds = new OptimizedWorker[workers.length];
            for (int i = 0; i < workers.length; i++) {
                optimizeds[i] = optimizeLocal(workers[i], optimizationTS);
            }

            LinkedList<OptimizedWorker> donors = new LinkedList<OptimizedWorker>();
            LinkedList<OptimizedWorker> receivers = new LinkedList<OptimizedWorker>();
            determineDonorsAndReceivers(optimizeds, donors, receivers);

            long difference = OPTIMIZATION_THRESHOLD - (System.currentTimeMillis() - lastUpdate);
            if (difference > 0) {
                try {
                    Thread.sleep(difference);
                } catch (InterruptedException ie) {
                    //Do nothing. Wake up in case of shutdown received
                }
            }
            lastUpdate = optimizationTS;
        }
        sem.release();
    }

    public void shutdown() throws InterruptedException {
        stop = true;
        this.interrupt();
        sem.acquire();
    }

    public OptimizedWorker optimizeLocal(ResourceScheduler<?> worker, long updateTimeStamp) {
        DefaultResourceScheduler drs = (DefaultResourceScheduler) worker;
        //PriorityQueue[] actions = drs.seekGaps(updateTimeStamp, new LinkedList<Gap>());
        drs.seekGaps(updateTimeStamp, new LinkedList<Gap>());
        long PI = ((DefaultResourceScheduler) worker).getPerformanceIndicator();
        return new OptimizedWorker(worker, PI);
    }

    public void optimize(ResourceScheduler<?>[] workers, long updateTimeStamp) {
        //Update Resource end Times
        long avgPI = 0;
        try {
            if (workers.length == 0) {
                return;
            }
            System.out.println("Optimizing @ " + updateTimeStamp);
            //Local Optimization
            for (ResourceScheduler<?> rs : workers) {
                DefaultResourceScheduler drs = (DefaultResourceScheduler) rs;
                drs.seekGaps(updateTimeStamp, new LinkedList<Gap>());
                long PI = ((DefaultResourceScheduler) rs).getPerformanceIndicator();
                avgPI += PI;
            }
            avgPI = avgPI / workers.length;

            LinkedList<OptimizedWorker> donors = new LinkedList<OptimizedWorker>();
            LinkedList<OptimizedWorker> receivers = new LinkedList<OptimizedWorker>();
            for (ResourceScheduler<?> rs : workers) {
                long PI = ((DefaultResourceScheduler) rs).getPerformanceIndicator();
                OptimizedWorker ow = new OptimizedWorker(rs, PI);
                if (avgPI != PI) {
                    if (avgPI > PI) {
                        receivers.add(ow);
                    } else {
                        donors.add(ow);
                    }
                }
            }

            for (OptimizedWorker donor : donors) {
                LinkedList<AllocatableAction> donedActions = donor.getExtraTasks();
                for (AllocatableAction action : donedActions) {
                    for (OptimizedWorker receptor : receivers) {
                        if (action.isCompatible(receptor.rs.getResource())) {
                            System.out.println("Pot mirar de donar-se");
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void determineDonorsAndReceivers(OptimizedWorker[] workers, LinkedList<OptimizedWorker> donors, LinkedList<OptimizedWorker> receivers) {
        long avgPI = 0;
        for (OptimizedWorker rs : workers) {
            avgPI += rs.performanceIndicator;
        }

        for (OptimizedWorker ow : workers) {
            if (avgPI != ow.performanceIndicator) {
                if (avgPI > ow.performanceIndicator) {
                    receivers.add(ow);
                } else {
                    donors.add(ow);
                }
            }
        }
    }

    private class OptimizedWorker {

        long performanceIndicator;
        ResourceScheduler<?> rs;

        public OptimizedWorker(ResourceScheduler<?> rs, long performanceIndicator) {
            this.performanceIndicator = performanceIndicator;
            this.rs = rs;
        }

        private LinkedList<AllocatableAction> getExtraTasks() {
            return new LinkedList<AllocatableAction>();
        }

    }

}
