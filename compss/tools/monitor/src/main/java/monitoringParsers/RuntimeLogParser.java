package monitoringParsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bsc.compss.commons.Loggers;
import com.bsc.compss.ui.Constants;
import com.bsc.compss.ui.Properties;
import com.bsc.compss.ui.ExecutionInformationTask;


public class RuntimeLogParser {

    private static Vector<ExecutionInformationTask> tasks = new Vector<>();
    private static Vector<ExecutionInformationTask> tasksCurrent = new Vector<>();
    private static Vector<ExecutionInformationTask> tasksFailed = new Vector<>();
    private static Vector<ExecutionInformationTask> tasksWithFailedJobs = new Vector<>();

    private static Vector<Integer> jobsToTasks = new Vector<>();
    private static Vector<String> resubmitedJobs = new Vector<>();

    private static String runtimeLogPath = "";
    private static int lastParsedLine = -1;

    private static final Logger logger = LogManager.getLogger(Loggers.RUNTIME_LOG_PARSER);


    public static Vector<ExecutionInformationTask> getTasks() {
        return tasks;
    }

    public static Vector<ExecutionInformationTask> getTasksCurrent() {
        return tasksCurrent;
    }

    public static Vector<ExecutionInformationTask> getTasksFailed() {
        return tasksFailed;
    }

    public static Vector<ExecutionInformationTask> getTasksWithFailedJobs() {
        return tasksWithFailedJobs;
    }

    public static void parse() {
        logger.debug("Parsing runtime.log file...");
        if (!Properties.getBasePath().equals("")) {
            // Check if application has changed
            String newPath = Properties.getBasePath() + File.separator + Constants.RUNTIME_LOG;
            if (!runtimeLogPath.equals(newPath)) {
                // Load new application
                clear();
                runtimeLogPath = newPath;
            }

            // Parse
            try ( FileReader fr = new FileReader(runtimeLogPath);
                    BufferedReader br = new BufferedReader(fr)) {

                String line = br.readLine(); // Parsed line
                int i = 0; // Line counter
                String lastNewJobId = new String(""); // Last new job ID entry
                while (line != null) { // TODO add transfer files status
                    if (i > lastParsedLine) {
                        // Check line information and add to structures
                        if (line.contains("@processTask") && (line.contains("New method task") || line.contains("New service task"))) {
                            logger.debug("* New task");
                            String[] str = line.split(" ");
                            String taskId = str[str.length - 1];
                            String taskName = line.substring(line.lastIndexOf("task(") + 5, line.lastIndexOf(")"));
                            if (Integer.valueOf(taskId) >= tasks.size()) {
                                tasks.setSize(Integer.valueOf(taskId) + 1);
                            }
                            tasks.set(Integer.valueOf(taskId), new ExecutionInformationTask(taskName, taskId));
                            if (!tasksCurrent.contains(tasks.get(Integer.valueOf(taskId)))) {
                                tasksCurrent.add(tasks.get(Integer.valueOf(taskId)));
                            }
                        } else if ((line.contains("@doError")) && (line.contains("rescheduling task execution"))) {
                            logger.debug(" * Rescheduled task");
                            String[] info = line.split(" ");
                            String taskId = info[info.length - 12];
                            tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_CREATING);
                        } else if (line.contains("@endTask") && line.contains(" with end status ")) {
                            logger.debug("* Task End");
                            String taskId = line.substring(line.lastIndexOf("task ") + 5, line.lastIndexOf(" with end status"));
                            String state = line.substring(line.lastIndexOf(" with end status ") + 17);
                            if (state.equals("FINISHED")) {
                                tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_DONE);
                            } else {
                                tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_FAILED);
                                if (!tasksFailed.contains(tasks.get(Integer.valueOf(taskId)))) {
                                    tasksFailed.add(tasks.get(Integer.valueOf(taskId)));
                                }
                            }
                            tasksCurrent.remove(tasks.get(Integer.valueOf(taskId)));
                        } else if ((line.contains("@errorOnAction")) && (line.contains("Blocked Action"))) {
                            logger.debug("* Blocked Action");
                            // Task is Blocked (we mark it as failed)
                            String taskId = line.substring(line.lastIndexOf("Task ") + 5, line.lastIndexOf(", CE name"));
                            tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_FAILED);
                            if (!tasksFailed.contains(tasks.get(Integer.valueOf(taskId)))) {
                                tasksFailed.add(tasks.get(Integer.valueOf(taskId)));
                            }
                            tasksCurrent.remove(tasks.get(Integer.valueOf(taskId)));
                        } else if ((line.contains("@doSubmit")) && (line.contains("New Job"))) {
                            logger.debug("* New job");
                            String[] str = line.split(" ");
                            lastNewJobId = str[str.length - 3];
                            String taskId = str[str.length - 1].substring(0, str[str.length - 1].indexOf(")"));
                            if (Integer.valueOf(lastNewJobId) >= jobsToTasks.size()) {
                                jobsToTasks.setSize(Integer.valueOf(lastNewJobId) + 1);
                            }
                            jobsToTasks.set(Integer.valueOf(lastNewJobId), Integer.valueOf(taskId));
                            tasks.get(Integer.valueOf(taskId)).addJob(lastNewJobId, false);
                            // Set job and task as running
                            tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_RUNNING);
                            tasks.get(Integer.valueOf(taskId)).setJobStatus(Constants.STATUS_TASK_RUNNING);
                            if (!tasksCurrent.contains(tasks.get(Integer.valueOf(taskId)))) {
                                tasksCurrent.add(tasks.get(Integer.valueOf(taskId)));
                            }
                        } else if ((line.contains("@doSubmit")) && (line.contains("Rescheduled Job"))) {
                            logger.debug("* Rescheduled job");
                            String[] str = line.split(" ");
                            lastNewJobId = str[str.length - 3];
                            String taskId = str[str.length - 1].substring(0, str[str.length - 1].indexOf(")"));
                            if (Integer.valueOf(lastNewJobId) >= jobsToTasks.size()) {
                                jobsToTasks.setSize(Integer.valueOf(lastNewJobId) + 1);
                            }
                            jobsToTasks.set(Integer.valueOf(lastNewJobId), Integer.valueOf(taskId));
                            tasks.get(Integer.valueOf(taskId)).addJob(lastNewJobId, false);
                            // Set job as running
                            tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_RUNNING);
                            tasks.get(Integer.valueOf(taskId)).setJobStatus(Constants.STATUS_TASK_RUNNING);
                            if (!tasksCurrent.contains(tasks.get(Integer.valueOf(taskId)))) {
                                tasksCurrent.add(tasks.get(Integer.valueOf(taskId)));
                            }
                        } else if ((line.contains("@doSubmit")) && (line.contains("* Target host"))) {
                            logger.debug("* Add target for last new job");
                            String host = line.substring(line.lastIndexOf(": ") + 1);
                            tasks.get(jobsToTasks.get(Integer.valueOf(lastNewJobId))).setJobHost(lastNewJobId, false, host);
                        } else if ((line.contains("@failedJob")) && (line.contains("with state FAILED"))) {
                            logger.debug("* Failed job");
                            String[] info = line.split(" ");
                            String jobId = info[info.length - 4];
                            tasks.get(jobsToTasks.get(Integer.valueOf(jobId))).setJobStatus(Constants.STATUS_TASK_FAILED);
                            if (!tasksWithFailedJobs.contains(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))))) {
                                tasksWithFailedJobs.add(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))));
                            }
                        } else if ((line.contains("@failedJob")) && (line.contains("resubmitting task to the same worker"))) {
                            logger.debug("* Job Resubmited");
                            String[] info = line.split(" ");
                            String jobId = info[info.length - 16];
                            String taskId = info[info.length - 12];
                            tasks.get(Integer.valueOf(taskId)).addJob(jobId, true);
                            tasks.get(Integer.valueOf(taskId)).setJobStatus(Constants.STATUS_TASK_RUNNING);
                            if (!tasksWithFailedJobs.contains(tasks.get(Integer.valueOf(taskId)))) {
                                tasksWithFailedJobs.add(tasks.get(Integer.valueOf(taskId)));
                            }
                            resubmitedJobs.add(jobId);
                        } else if ((line.contains("@completedJob")) && (line.contains("with state OK"))) {
                            logger.debug("* Job completed");
                            String[] info = line.split(" ");
                            String jobId = info[info.length - 4];
                            tasks.get(jobsToTasks.get(Integer.valueOf(jobId))).setJobStatus(Constants.STATUS_TASK_DONE);
                        }
                    }
                    i = i + 1;
                    line = br.readLine();
                }
                lastParsedLine = i - 1;
            } catch (Exception e) {
                logger.error("Cannot parse runtime.log file: " + runtimeLogPath);
                logger.debug("Error ", e);
                clear();
            }
        } else {
            // Load default value
            clear();
        }
        logger.debug("runtime.log file parsed");
    }

    private static void clear() {
        tasks.clear();
        tasksCurrent.clear();
        tasksFailed.clear();
        tasksWithFailedJobs.clear();

        runtimeLogPath = "";
        lastParsedLine = -1;
        jobsToTasks.clear();
        resubmitedJobs.clear();
    }

}
