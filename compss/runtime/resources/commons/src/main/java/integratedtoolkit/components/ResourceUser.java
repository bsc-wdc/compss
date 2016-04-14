package integratedtoolkit.components;

import integratedtoolkit.types.resources.Worker;


public interface ResourceUser {

    public void createdResources(Worker<?> r);

    public WorkloadStatus getWorkload();

    public class WorkloadStatus {

        //Core Information
        private int coreCount;
        private long[] coreMinTime;
        private long[] coreMeanTime;
        private long[] coreMaxTime;
        
        //Task counters
        private int noResourceCount;
        private int[] noResourceCounts;
        private int ordinaryCount;
        private int[] ordinaryCounts;
        private int toRescheduleCount;
        private int[] toRescheduleCounts;
        
        //Running Tasks
        private int[] runningCounts;
        private long[] runningCoreMeanTime;
        
        //Wait Counts
        private int[] waitingCounts;

        public WorkloadStatus(int coreCount) {
            this.coreCount = coreCount;
            coreMinTime = new long[coreCount];
            coreMeanTime = new long[coreCount];
            coreMaxTime = new long[coreCount];

            noResourceCounts = new int[coreCount];
            ordinaryCounts = new int[coreCount];
            toRescheduleCounts = new int[coreCount];

            waitingCounts = new int[coreCount];
            
            runningCounts = new int[coreCount];
            runningCoreMeanTime = new long[coreCount];
           
        }

        public int getCoreCount() {
            return coreCount;
        }

        public long getCoreMeanTime(int coreId) {
            return coreMeanTime[coreId];
        }

        public long getCoreMaxTime(int coreId) {
            return coreMaxTime[coreId];
        }

        public long getCoreMinTime(int coreId) {
            return coreMinTime[coreId];
        }

        public int getNoResourceCount() {
            return noResourceCount;
        }

        public int[] getNoResourceCounts() {
            return noResourceCounts;
        }

        public int getReadyTaskCount() {
            return noResourceCount + ordinaryCount + toRescheduleCount;
        }
        
        public int[] getReadyTaskCounts() {
            int[] counts = new int[coreCount];
            for (int coreId = 0; coreId < coreCount; coreId++) {
                counts[coreId] = noResourceCounts[coreId] + ordinaryCounts[coreId] + toRescheduleCounts[coreId];
            }
            return counts;
        }

        public int getOrdinaryCount() {
            return ordinaryCount;
        }
        
        public int[] getOrdinaryCounts() {
            return ordinaryCounts;
        }

        public int[] getWaitingTaskCounts() {
            return waitingCounts;
        }
        
        public long getRunningCoreMeanTime(int coreId) {
            return runningCoreMeanTime[coreId];
        }

        public int getRunningTaskCount() {
            int sum = 0;
            for(int x : runningCounts) {
            	sum += x;
            }
            return sum;
        }
        
        public int[] getRunningTaskCounts() {
            return runningCounts;
        }
        

        public String toString() {
            StringBuilder sb = new StringBuilder();
            //Time Stamp
            sb.append("TIMESTAMP = ").append(String.valueOf(System.currentTimeMillis())).append("\n");

            //Load Information
            sb.append("LOAD_INFO = [").append("\n");
            for (int coreId = 0; coreId < coreCount; coreId++) {
                sb.append("\t").append("CORE_INFO = [").append("\n");
                sb.append("\t").append("\t").append("COREID = ").append(coreId).append("\n");
                sb.append("\t").append("\t").append("NO_RESOURCE = ").append(noResourceCounts[coreId]).append("\n");
                sb.append("\t").append("\t").append("TO_RESCHEDULE = ").append(toRescheduleCounts[coreId]).append("\n");
                sb.append("\t").append("\t").append("ORDINARY = ").append(ordinaryCounts[coreId]).append("\n"); 
                sb.append("\t").append("\t").append("RUNNING = ").append(runningCounts[coreId]).append("\n");	
                sb.append("\t").append("\t").append("MIN = ").append(coreMinTime[coreId]).append("\n");
                sb.append("\t").append("\t").append("MEAN = ").append(coreMeanTime[coreId]).append("\n");
                sb.append("\t").append("\t").append("MAX = ").append(coreMaxTime[coreId]).append("\n");
                sb.append("\t").append("\t").append("RUNNING_MEAN = ").append(runningCoreMeanTime[coreId]).append("\n");
                sb.append("\t").append("]").append("\n");
            }
            sb.append("]").append("\n");

            return sb.toString();
        }

        public void registerTimes(int coreId, long minTime, long avgTime, long maxTime) {
            if (coreId < coreCount) {
                coreMinTime[coreId] = minTime;
                coreMeanTime[coreId] = avgTime;
                coreMaxTime[coreId] = maxTime;
            }
        }

        public void registerReadyTaskCounts(int coreId, int noResource, int regular, int toReschedule) {
            this.noResourceCount += noResource;
            this.ordinaryCount += regular;
            this.toRescheduleCount += toReschedule;

            this.noResourceCounts[coreId] = noResource;
            this.ordinaryCounts[coreId] = regular;
            this.toRescheduleCounts[coreId] = toReschedule;
        }
        
        public void registerRunningTask(int coreId, int runningTasks, long meanRunningTime) {
            
        	this.runningCoreMeanTime[coreId] = meanRunningTime;
            this.runningCounts[coreId] = runningTasks;
        }
    }

}
