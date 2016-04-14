package com.bsc.compss.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.zkoss.zul.ListModelList;


public class ExecutionInformationTask {
	private String color;
	
	private String name;
    private String taskId;
    private String status;
    private ArrayList<Job> jobs;
    
    public ExecutionInformationTask(String name, String taskId) {
    	this.setName(name);										//Any
    	this.setTaskId(taskId);									//Any
    	this.setTaskStatus(Constants.STATUS_TASK_CREATING);		//CONST_VALUES
    	jobs = new ArrayList<Job>();
    }
    
	public String getStatus() {
		return status;
	}

	public void setTaskStatus(String status) {
		if (status.equals(Constants.STATUS_TASK_CREATING)) {
			this.status = status;
			this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_CREATING + ".jpg");
		} else if (status.equals(Constants.STATUS_TASK_RUNNING)) {
			this.status = status;
			this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_RUNNING + ".jpg");
		} else if (status.equals(Constants.STATUS_TASK_DONE)) {
			this.status = status;
			this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_DONE + ".jpg");
		} else if (status.equals(Constants.STATUS_TASK_FAILED)) {
			this.status = status;
			this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_FAILED + ".jpg");
		} else {
			//Default value for error
			this.status = Constants.STATUS_TASK_FAILED;
			this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_FAILED + ".jpg");
		}
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public String getTaskId() {
		return this.taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	
	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}
	
    public List<Job> getJobs () {
    	return new ListModelList<Job>(this.jobs);
    }
	
	public void addJob (String jobId, boolean resubmited) {
		if (resubmited) {
			jobId = jobId + "R";
		}
		jobs.add(new Job(jobId, resubmited));
	}
	
	public void setJobStatus (String jobId, boolean resubmited, String status) {
		if (resubmited) {
			jobId = jobId + "R";
		}
		for (Job j : this.jobs) {
			if (j.getId().equals(jobId)) {
				j.setStatus(status);
			}
		}
	}
	
	public void setJobHost (String jobId, boolean resubmited, String host) {
		if (resubmited) {
			jobId = jobId + "R";
		}
		for (Job j : this.jobs) {
			if (j.getId().equals(jobId)) {
				j.setHost(host);
			}
		}
	}
	
	
	public class Job {
		private String id;
		private boolean resubmited;
		private String host;
		private String status;
		private String executable;
		private String arguments;
		private String color;
	
		public Job() {
			id = new String("");
			resubmited = false;
			host = new String("");
			status = new String(Constants.STATUS_CREATION);
			color = new String(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_CREATING + ".jpg");
			executable = new String("");
			arguments = new String("");
		}
		
		public Job(String id, boolean resubmited) {
			this.id = id;
			this.resubmited = resubmited;
			this.host = new String("");
			status = new String(Constants.STATUS_CREATION);
			color = new String(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_CREATING + ".jpg");
			executable = new String("");
			arguments = new String("");
		}
		
		public String getId() {
			return this.id;
		}
		
		public void setId(String id) {
			this.id = id;
		}
		
		public String getHost() {
			return this.host;
		}
		
		public void setHost(String host) {
			this.host = host;
		}
		
		public String getStatus() {
			return this.status;
		}
		
		public void setStatus(String status) {
			if (status.equals(Constants.STATUS_TASK_CREATING)) {
				this.status = status;
				this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_CREATING + ".jpg");
			} else if (status.equals(Constants.STATUS_TASK_RUNNING)) {
				this.status = status;
				this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_RUNNING + ".jpg");
			} else if (status.equals(Constants.STATUS_TASK_DONE)) {
				this.status = status;
				this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_DONE + ".jpg");
			} else if (status.equals(Constants.STATUS_TASK_FAILED)) {
				this.status = status;
				this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_FAILED + ".jpg");
			} else {
				//Default value for error
				this.status = Constants.STATUS_TASK_FAILED;
				this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_TASK_FAILED + ".jpg");
			}
		}
		
		public String getColor() {
			return color;
		}

		public void setColor(String color) {
			this.color = color;
		}
		
		public String getExecutable() {
			executable = new String("Not Available");
			if (!Properties.BASE_PATH.equals("")) {
				BufferedReader br = null;
				try {
					String jobOutPath;
					if (!this.resubmited) {
						jobOutPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id + Constants.JOB_OUT_FILE;
					} else {
						jobOutPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id.substring(0, this.id.length()-1) + Constants.JOB_OUT_RESUBMITTED_FILE;
					}
					
					br = new BufferedReader(new FileReader(jobOutPath));
					String line = br.readLine();
					boolean found = false;
					while ((line != null) && (!found)) {
						if (line.contains("* Executable:")) {
							found = true;
							executable = line.substring(line.lastIndexOf("* Executable:") + 13);
						}
						line = br.readLine();
					}
				} catch (Exception e) {
					//Out file doesn't exist - Display shows no information. No need to raise exception
				} finally {
					try {
						if (br != null) {
							br.close();
						}
					} catch (Exception e) {
						//Out file cannot close - Display shows no information. No need to raise exception
					}
				}
			}
			
			return executable;
		}
		
		public String getArguments() {
			arguments = new String("Not Available");
			if (!Properties.BASE_PATH.equals("")) {
				BufferedReader br = null;
				try {
					String jobOutPath;
					if (!this.resubmited) {
						jobOutPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id + Constants.JOB_OUT_FILE;
					} else {
						jobOutPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id.substring(0, this.id.length()-1) + Constants.JOB_OUT_RESUBMITTED_FILE;
					}
					
					br = new BufferedReader(new FileReader(jobOutPath));
					String line = br.readLine();
					boolean found = false;
					while ((line != null) && (!found)) {
						if (line.contains("* Parameter values:")) {
							found = true;
							arguments = line.substring(line.lastIndexOf("* Parameter values:") + "* Parameter values:".length());
						}
						line = br.readLine();
					}
				} catch (Exception e) {
					//Out file doesn't exist - Display shows no information. No need to raise exception
				} finally {
					try {
						if (br != null) {
							br.close();
						}
					} catch (Exception e) {
						//Out file cannot close - Display shows no information. No need to raise exception
					}
				}
			}
			
			return arguments;
		}
		
		public String getOutFileContent() {
			if (!Properties.BASE_PATH.equals("")) {
				StringBuilder sb = new StringBuilder();
				try {
					String jobOutPath;
					if (!this.resubmited) {
						jobOutPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id + Constants.JOB_OUT_FILE;
					} else {
						jobOutPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id.substring(0, this.id.length()-1) + Constants.JOB_OUT_RESUBMITTED_FILE;
					}
					BufferedReader br = new BufferedReader(new FileReader(jobOutPath));
					String line = br.readLine();
					while (line != null) {
						sb.append(line).append("\n");
						line = br.readLine();
					}
					br.close();
				} catch (Exception e) {
					//Out file doesn't exist
					return new String("Not Available. Set log-level to debug.");
				}
				return sb.toString();
			}
			return new String("");
		}
		
		public String getErrFileContent() {
			if (!Properties.BASE_PATH.equals("")) {
				StringBuilder sb = new StringBuilder();
				try {
					String jobErrPath;
					if (!this.resubmited) {
						jobErrPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id + Constants.JOB_ERR_FILE;
					} else {
						jobErrPath = Properties.BASE_PATH + Constants.JOBS_SUB_PATH + "job" + this.id.substring(0, this.id.length()-1) + Constants.JOB_ERR_RESUBMITTED_FILE;
					}
					BufferedReader br = new BufferedReader(new FileReader(jobErrPath));
					String line = br.readLine();
					while (line != null) {
						sb.append(line).append("\n");
						line = br.readLine();
					}
					br.close();
				} catch (Exception e) {
					//Err file doesn't exist
					return new String("Not Available. Set log-level to debug.");
				}
				return sb.toString();
			}
			return new String("");
		}
	}

}
