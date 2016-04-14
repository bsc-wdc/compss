package monitoringParsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Vector;
import org.apache.log4j.Logger;

import com.bsc.compss.ui.Constants;
import com.bsc.compss.ui.Properties;
import com.bsc.compss.ui.ExecutionInformationTask;


public class RuntimeLogParser {
	private static Vector<ExecutionInformationTask> tasks = new Vector<ExecutionInformationTask>();
	private static Vector<ExecutionInformationTask> tasksCurrent = new Vector<ExecutionInformationTask>();
	private static Vector<ExecutionInformationTask> tasksFailed = new Vector<ExecutionInformationTask>();
	private static Vector<ExecutionInformationTask> tasksWithFailedJobs = new Vector<ExecutionInformationTask>();
	private static String runtimeLogPath = "";
	private static int lastParsedLine = -1;
	private static Vector<Integer> jobsToTasks = new Vector<Integer>();
	private static Vector<String> resubmitedJobs = new Vector<String>();
	
	private static final Logger logger = Logger.getLogger("compssMonitor.monitoringParser");

	
	public static Vector<ExecutionInformationTask> getTasks () {
		return tasks;
	}
	
	public static Vector<ExecutionInformationTask> getTasksCurrent () {
		return tasksCurrent;
	}
	
	public static Vector<ExecutionInformationTask> getTasksFailed () {
		return tasksFailed;
	}
	
	public static Vector<ExecutionInformationTask> getTasksWithFailedJobs () {
		return tasksWithFailedJobs;
	}
	
	public static void parse () {
		logger.debug("Parsing it.log file...");
    	if (!Properties.BASE_PATH.equals("")) {
    		//Check if applicaction has changed
    		String newPath = Properties.BASE_PATH + File.separator + Constants.RUNTIME_LOG;
    		if (!runtimeLogPath.equals(newPath)) {
    			//Load new application
    			clear();
    			runtimeLogPath = newPath;
    		}
    		//Parse
    		try {
    			FileReader fr = new FileReader(runtimeLogPath);
    			BufferedReader br = new BufferedReader(fr);
    			String line = br.readLine();				//Parsed line
    			int i = 0;									//Line counter
    			String lastNewJobId = new String("");		//Last new job ID entry
    			while (line != null) {						//TODO add transfer files status
    				if (i > lastParsedLine) {
    					//Check line information and add to structures
    					if (line.contains("@processTask") && (line.contains("New method task") || line.contains("New service task"))) {
    						logger.debug("* New task");
    						String[] str = line.split(" ");
    						String taskId = str[str.length - 1];
    						String taskName = line.substring(line.lastIndexOf("task(") + 5, line.lastIndexOf(")"));
    						if (Integer.valueOf(taskId) >= tasks.size()) {
    							tasks.setSize(Integer.valueOf(taskId) + 1); 
    						}
    						tasks.set(Integer.valueOf(taskId), new ExecutionInformationTask(taskName, taskId));
    						
    					} else if ((line.contains("@processJob")) && (line.contains("New Job"))) {
    						logger.debug("* New job");
    						String[] str = line.split(" ");
    						lastNewJobId = str[str.length - 3];
    						String taskId = str[str.length - 1].substring(0, str[str.length -1].indexOf(")"));
    						if (Integer.valueOf(lastNewJobId) >= jobsToTasks.size()) {
    							jobsToTasks.setSize(Integer.valueOf(lastNewJobId) + 1); 
    						}
    						jobsToTasks.set(Integer.valueOf(lastNewJobId), Integer.valueOf(taskId));
    						tasks.get(Integer.valueOf(taskId)).addJob(lastNewJobId, false);
    					} else if ((line.contains("@processJob")) && (line.contains("* Target host"))) {
    						logger.debug("* Add target for last new job");
    						String host = line.substring(line.lastIndexOf(": ") + 1);
    						tasks.get(jobsToTasks.get(Integer.valueOf(lastNewJobId))).setJobHost(lastNewJobId, false, host);
    	
    					} else if (line.contains("@submit") && line.contains("Submit ") && line.contains("Job with ID")) {
    						logger.debug("* Job submited");
    						String[] str = line.split(" ");
    						String jobId = str[str.length - 1];
    						tasks.get(jobsToTasks.get(Integer.valueOf(jobId))).setJobStatus(jobId, resubmitedJobs.contains(jobId), Constants.STATUS_TASK_RUNNING);
    						if (!tasksCurrent.contains(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))))) {
    							tasksCurrent.add(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))));
    						}
    					} else if (line.contains("@failedJob") && line.contains("Resubmitting job ")) {
    						logger.debug("* Job Resubmited");
    						String jobId = line.substring(line.lastIndexOf("Resubmitting job ") + "Resubmitting job ".length() , line.lastIndexOf(" to "));
    						tasks.get(jobsToTasks.get(Integer.valueOf(jobId))).addJob(jobId, true);
    						resubmitedJobs.add(jobId);
    					} 
    					
    					else if (line.contains("@completedJob") && line.contains("Received a notification for job") && line.contains("with state ")) {
    						logger.debug("* Job completed Notification");
    						String jobId = line.substring(line.lastIndexOf("job ") + 4, line.lastIndexOf(" with state"));
    						String state = line.substring(line.lastIndexOf(" with state ") + 12);
    						if (state.equals("OK")) {
    							tasks.get(jobsToTasks.get(Integer.valueOf(jobId))).setJobStatus(jobId, resubmitedJobs.contains(jobId), Constants.STATUS_TASK_DONE);
    						} else {
    							tasks.get(jobsToTasks.get(Integer.valueOf(jobId))).setJobStatus(jobId, resubmitedJobs.contains(jobId), Constants.STATUS_TASK_FAILED);
    							if (!tasksWithFailedJobs.contains(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))))) {
    								tasksWithFailedJobs.add(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))));
    							}
    						}
    					}
    					else if (line.contains("@failedJob") && line.contains("Received a notification for job") && line.contains("with state ")) {
    						logger.debug("* Job failed Notification");
    						String jobId = line.substring(line.lastIndexOf("job ") + 4, line.lastIndexOf(" with state"));
    						//String state = line.substring(line.lastIndexOf(" with state ") + 12);
    						//if (state.equals("FAILED")) {
							tasks.get(jobsToTasks.get(Integer.valueOf(jobId))).setJobStatus(jobId, resubmitedJobs.contains(jobId), Constants.STATUS_TASK_FAILED);
							if (!tasksWithFailedJobs.contains(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))))) {
								tasksWithFailedJobs.add(tasks.get(jobsToTasks.get(Integer.valueOf(jobId))));
							}
    					} 
    					
    					else if (line.contains("@notifyTaskEnd")) {
    						logger.debug("* Task End");
    						String taskId = line.substring(line.lastIndexOf("task ") + 5, line.lastIndexOf(" with end status"));
    						String state = line.substring(line.lastIndexOf(" with end status ") + 17);
    						if (state.equals("FINISHED")) {
    							tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_DONE);
    						} else {
    							tasks.get(Integer.valueOf(taskId)).setTaskStatus(Constants.STATUS_TASK_FAILED);
    							tasksFailed.add(tasks.get(Integer.valueOf(taskId)));
    						}
    						tasksCurrent.remove(tasks.get(Integer.valueOf(taskId)));
    					}
    				}
    				i = i + 1;
    				line = br.readLine();
    			}
    			lastParsedLine = i - 1;
    			br.close();
    			fr.close();
    		} catch (Exception e) {
    			clear();
    			logger.error("Cannot parse runtime.log file: " + runtimeLogPath);
    		}
    	} else {
    		//Load default value
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
