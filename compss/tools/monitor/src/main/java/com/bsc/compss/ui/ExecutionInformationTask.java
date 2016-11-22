package com.bsc.compss.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.zkoss.zul.ListModelList;


public class ExecutionInformationTask {
    
    private static final String STATE_IMAGES_RELATIVE_PATH = File.separator + "images" + File.separator + "state" + File.separator;
    private static final String STATE_IMAGES_EXTENSION = ".jpg";
    private static final String JOBS_RELATIVE_PATH = Constants.JOBS_SUB_PATH + "job";

    private String color;

    private String name;
    private String taskId;
    private String status;
    private ArrayList<Job> jobs;


    public ExecutionInformationTask(String name, String taskId) {
        this.setName(name); // Any
        this.setTaskId(taskId); // Any
        this.setTaskStatus(Constants.STATUS_TASK_CREATING); // CONST_VALUES
        jobs = new ArrayList<>();
    }

    public String getStatus() {
        return status;
    }

    public void setTaskStatus(String status) {
        if (status.equals(Constants.STATUS_TASK_CREATING)) {
            this.status = status;
            this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_CREATING + STATE_IMAGES_EXTENSION);
        } else if (status.equals(Constants.STATUS_TASK_RUNNING)) {
            this.status = status;
            this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_RUNNING + STATE_IMAGES_EXTENSION);
        } else if (status.equals(Constants.STATUS_TASK_DONE)) {
            this.status = status;
            this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_DONE + STATE_IMAGES_EXTENSION);
        } else if (status.equals(Constants.STATUS_TASK_FAILED)) {
            this.status = status;
            this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_FAILED + STATE_IMAGES_EXTENSION);
        } else {
            // Default value for error
            this.status = Constants.STATUS_TASK_FAILED;
            this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_FAILED + STATE_IMAGES_EXTENSION);
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

    public List<Job> getJobs() {
        return new ListModelList<>(this.jobs);
    }

    public void addJob(String jobId, boolean resubmited) {
        Job job;

        if (!resubmited) {
            // Create a normal job
            job = new Job(jobId);
        } else {
            // Retrieve information about normal job
            String host = "";
            String executable = "";
            String args = "";
            // Reverse search because job is probabilistically at the end
            int i = jobs.size() - 1;
            boolean found = false;
            while (i >= 0 && !found) {
                Job jNormal = jobs.get(i);
                if (jNormal.getId().equals(jobId)) {
                    host = jNormal.getHost();
                    executable = jNormal.getExecutable();
                    args = jNormal.getArguments();
                    found = true;
                }
                i = i - 1;
            }
            // Create a resubmitted job
            String newJobId = jobId + "R";
            job = new Job(newJobId, resubmited, host, executable, args);
        }

        // Add job
        if (!jobs.contains(job)) { // Protection added for UI update
            jobs.add(job);
        }
    }

    public void setJobStatus(String status) {
        this.jobs.get(this.jobs.size() - 1).setStatus(status);
    }

    public void setJobHost(String jobId, boolean resubmited, String host) {
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

        private final String id;
        private final boolean resubmited;
        private String host;
        private String status;
        private String executable;
        private String arguments;
        private String color;


        public Job(String id) {
            this.id = id;
            this.resubmited = false;
            this.host = new String("");
            this.status = new String(Constants.STATUS_CREATION);
            this.color = new String(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_CREATING + STATE_IMAGES_EXTENSION);
            this.executable = new String("");
            this.arguments = new String("");
        }

        public Job(String id, boolean resubmited, String host, String executable, String args) {
            this.id = id;
            this.resubmited = resubmited;
            this.host = host;
            this.status = new String(Constants.STATUS_CREATION);
            this.color = new String(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_CREATING + STATE_IMAGES_EXTENSION);
            this.executable = executable;
            this.arguments = args;
        }

        public String getId() {
            return this.id;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Job)) {
                return false;
            }

            Job job2 = (Job) obj;
            return this.getId().equals(job2.getId());
        }
        
        @Override
        public int hashCode() {
            return Integer.valueOf(this.getId());
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
                this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_CREATING + STATE_IMAGES_EXTENSION);
            } else if (status.equals(Constants.STATUS_TASK_RUNNING)) {
                this.status = status;
                this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_RUNNING + STATE_IMAGES_EXTENSION);
            } else if (status.equals(Constants.STATUS_TASK_DONE)) {
                this.status = status;
                this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_DONE + STATE_IMAGES_EXTENSION);
            } else if (status.equals(Constants.STATUS_TASK_FAILED)) {
                this.status = status;
                this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_FAILED + STATE_IMAGES_EXTENSION);
            } else {
                // Default value for error
                this.status = Constants.STATUS_TASK_FAILED;
                this.setColor(STATE_IMAGES_RELATIVE_PATH + Constants.COLOR_TASK_FAILED + STATE_IMAGES_EXTENSION);
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
            if (!Properties.getBasePath().isEmpty()) {
                String jobOutPath;
                if (!this.resubmited) {
                    jobOutPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id + Constants.JOB_OUT_FILE;
                } else {
                    jobOutPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id.substring(0, this.id.length() - 1)
                            + Constants.JOB_OUT_RESUBMITTED_FILE;
                }
                
                try (BufferedReader br = new BufferedReader(new FileReader(jobOutPath))) {
                    String line = br.readLine();
                    boolean found = false;
                    while ((line != null) && (!found)) {
                        if (line.contains("* Executable:")) {
                            found = true;
                            executable = line.substring(line.lastIndexOf("* Executable:") + 13);
                        }
                        line = br.readLine();
                    }
                } catch (IOException ioe) {
                    // Out file doesn't exist - Display shows no information. No need to raise exception
                }
            }

            return executable;
        }

        public String getArguments() {
            arguments = new String("Not Available");
            if (!Properties.getBasePath().isEmpty()) {
                String jobOutPath;
                if (!this.resubmited) {
                    jobOutPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id + Constants.JOB_OUT_FILE;
                } else {
                    jobOutPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id.substring(0, this.id.length() - 1)
                            + Constants.JOB_OUT_RESUBMITTED_FILE;
                }
                
                try (BufferedReader br = new BufferedReader(new FileReader(jobOutPath))) {
                    String line = br.readLine();
                    boolean found = false;
                    while ((line != null) && (!found)) {
                        if (line.contains("* Parameter values:")) {
                            found = true;
                            arguments = line.substring(line.lastIndexOf("* Parameter values:") + "* Parameter values:".length());
                        }
                        line = br.readLine();
                    }
                } catch (IOException ioe) {
                    // Out file doesn't exist - Display shows no information. No need to raise exception
                }
            }

            return arguments;
        }

        public String getOutFileContent() {
            if (!Properties.getBasePath().isEmpty()) {
                String jobOutPath;
                if (!this.resubmited) {
                    jobOutPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id + Constants.JOB_OUT_FILE;
                } else {
                    jobOutPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id.substring(0, this.id.length() - 1)
                            + Constants.JOB_OUT_RESUBMITTED_FILE;
                }
                
                StringBuilder sb = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new FileReader(jobOutPath))) {
                    String line = br.readLine();
                    while (line != null) {
                        sb.append(line).append("\n");
                        line = br.readLine();
                    }
                } catch (IOException ioe) {
                    // Out file doesn't exist
                    return new String("Not Available. Set log-level to debug.");
                }
                return sb.toString();
            }
            
            return new String("");
        }

        public String getErrFileContent() {
            if (!Properties.getBasePath().isEmpty()) {
                String jobErrPath;
                if (!this.resubmited) {
                    jobErrPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id + Constants.JOB_ERR_FILE;
                } else {
                    jobErrPath = Properties.getBasePath() + JOBS_RELATIVE_PATH + this.id.substring(0, this.id.length() - 1)
                            + Constants.JOB_ERR_RESUBMITTED_FILE;
                }
                
                StringBuilder sb = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new FileReader(jobErrPath))) {
                    String line = br.readLine();
                    while (line != null) {
                        sb.append(line).append("\n");
                        line = br.readLine();
                    }
                } catch (IOException ioe) {
                    // Out file doesn't exist
                    return new String("Not Available. Set log-level to debug.");
                }
                return sb.toString();
            }
            return new String("");
        }
    }

}
