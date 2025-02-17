/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.gos.master;

import es.bsc.compss.gos.master.sshutils.SSHHost;
import es.bsc.compss.gos.master.utils.ForbiddenCharacters;

import java.io.File;
import java.io.IOException;

import java.lang.Math;

import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.commons.lang3.StringUtils;


/**
 * The type Gos job description.
 */
public class GOSJobDescription {

    private String pathResponse;
    public ArrayList<String> arguments;
    private final ArrayList<String> argumentsKey;
    public ArrayList<String> killArguments;
    private String executable;
    private String commandOptionsBatch;
    private SSHHost host;
    private String outputError;

    private String sandboxDir;

    private String id;
    private String output;
    private int tracingSlot;
    private ArrayList<String> queues;
    private String cfg;
    private String qos;
    private String projectName;
    private long maxExecTime;
    private String reservation;
    private String killDir;
    private GOSJob job;


    public String getHostname() {
        return this.host.getFullHostName();
    }

    public SSHHost getSSHHost() {
        return this.host;
    }

    public String getOutputError() {
        return outputError;
    }

    public String getOutput() {
        return output;
    }

    public String getID() {
        return id;
    }

    public void setID(String id) {
        this.id = id;
    }

    public void setHost(SSHHost host) {
        this.host = host;
    }

    public String getExecutable() {
        return executable;
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    /**
     * Instantiates a new Gos job description.
     *
     * @param gosJob the job that this jobDescriptionDefines
     */
    public GOSJobDescription(GOSJob gosJob) {
        this.arguments = new ArrayList<>();
        this.argumentsKey = new ArrayList<>();
        this.job = gosJob;
        this.killArguments = new ArrayList<>();
        ForbiddenCharacters.init();
    }

    public ArrayList<String> getArguments() {
        return arguments;
    }

    /**
     * Add list to argument.
     *
     * @param values the values
     * @param message the message
     */
    public void addArgument(LinkedList<String> values, String message) {
        int i = 0;
        for (String v : values) {
            addArgument(message + i++, v);
        }
    }

    /**
     * Add argument to argument list. If the argument is empty records string "null", if the key is empty replaces with
     * key: "key_not_given".
     *
     * @param key the key
     * @param value the value
     */
    public void addArgument(String key, String value, boolean replaceSpace) {
        if (value == null || value.isEmpty() || value.equals("\"\"")) {
            value = "null";
        }
        if (key == null || key.isEmpty() || key.equals("\"\"")) {
            key = "key_not_given";
        }
        value = replaceIllegalCharacters(value, replaceSpace);
        argumentsKey.add(key);
        arguments.add(value);
    }

    public void addArgument(String key, String value) {
        addArgument(key, value, false);
    }

    /**
     * Replace illegal characters in arguments between to positions in the argument list, including both positions.
     *
     * @param initialPosition the initial position
     * @param finalPosition the final position
     */
    public void replaceIllegalCharacters(int initialPosition, int finalPosition) {
        for (int i = initialPosition; i <= finalPosition; i++) {
            String oldValue = arguments.get(i);
            String value = replaceIllegalCharacters(oldValue, true);
            if (!oldValue.equals(value)) {
                arguments.set(i, value);
            }
        }
    }

    private String replaceIllegalCharacters(String value, boolean replaceSpace) {
        return ForbiddenCharacters.encode(value, replaceSpace);
    }

    /**
     * Fill keys.
     *
     * @param fillMessage the message
     */
    public void fillKeys(String fillMessage) {
        int diff = arguments.size() - argumentsKey.size();
        for (int i = 0; i < diff; i++) {
            argumentsKey.add(fillMessage + i);
        }

    }

    public int numArgs() {
        return Math.min(arguments.size(), argumentsKey.size());
    }

    public String getArgKey(int x) {
        return argumentsKey.get(x);
    }

    public String getArg(int x) {
        return arguments.get(x);
    }

    /**
     * Add output.
     *
     * @param out the output file
     * @param err the error file
     */
    public void addOutput(String out, String err) throws IOException {
        this.output = out;
        this.outputError = err;
        createOutputFiles();
    }

    public void addTracingSlot(int slot) {
        this.tracingSlot = slot;
    }

    private void createOutputFiles() throws IOException {
        File newFile = new File(output);
        newFile.createNewFile();
        newFile = new File(outputError);
        newFile.createNewFile();
    }

    public String getSandboxDir() {
        return sandboxDir;
    }

    public void setSandboxDir(String sandboxDir) {
        this.sandboxDir = sandboxDir;
    }

    /**
     * Gets response file dir.
     *
     * @return the response file dir
     */
    public String getResponseFileDir() {
        if (pathResponse == null) {
            pathResponse = this.getSandboxDir() + GOSWorkerNode.SSH_RESPONSE_DIR;
        }
        return pathResponse;
    }

    /**
     * Gets kill script dir.
     *
     * @return the kill script dir
     */
    public String getCancelScriptDir() {
        if (killDir == null) {
            killDir = this.getSandboxDir() + GOSWorkerNode.CANCEL_JOB_DIR;
        }
        return killDir;
    }

    /**
     * Gets kill script.
     * 
     * @param compositeID Composite ID
     * @return The cancel script
     */
    public String getCancelScript(String compositeID) {
        String script = getCancelScriptDir() + "/" + compositeID;
        if (!killArguments.isEmpty()) {
            script = script + " " + StringUtils.join(killArguments, " ");
        }
        return script;
    }

    public void setQueueType(ArrayList<String> queues) {
        this.queues = queues;
    }

    public ArrayList<String> getQueues() {
        return this.queues;
    }

    /**
     * Sets cfg.
     *
     * @param fileCFG the file cfg
     */
    public void setCFG(Object fileCFG) {
        String t = (String) fileCFG;
        if (fileCFG == null || t.isEmpty() || t.equals("null")) {
            this.cfg = "";
        } else {
            this.cfg = t;
        }
    }

    public String getCFG() {
        return this.cfg;
    }

    /**
     * Sets qos.
     *
     * @param qos the qos
     */
    public void setQOS(Object qos) {
        String t = (String) qos;
        if (qos == null || t.isEmpty() || t.equals("null")) {
            this.qos = "false";
        } else {
            this.qos = t;
        }
    }

    public String getQOS() {
        return this.qos;
    }

    /**
     * Sets project name.
     *
     * @param projectName the project name
     */
    public void setProjectName(Object projectName) {
        String t = (String) projectName;
        if (projectName == null || t.isEmpty() || t.equals("null")) {
            this.projectName = "";
        } else {
            this.projectName = t;
        }
    }

    public String getProjectName() {
        return this.projectName;
    }

    /**
     * Sets max exec time.
     *
     * @param time the time
     */
    public void setMaxExecTime(Object time) {
        Long t = (Long) time;
        if (time == null || t < 1) {
            this.maxExecTime = 30;
        } else {
            this.maxExecTime = t;
        }
    }

    public long getMaxExecTime() {
        return this.maxExecTime;
    }

    /**
     * Sets reservation.
     *
     * @param reservation the reservation
     */
    public void setReservation(Object reservation) {
        String r = (String) reservation;
        if (reservation == null || r.isEmpty() || r.equals("null")) {
            this.reservation = "disabled";
        } else {
            this.reservation = r;
        }
    }

    public String getReservation() {
        return reservation;
    }

    public void setCommandArgsBatch(String args) {
        this.commandOptionsBatch = args;
    }

    public String getCommandArgsBatch() {
        return commandOptionsBatch;
    }

    /**
     * Get args string.
     *
     * @return the string
     */
    public String getArgumentsString() {
        String args = StringUtils.join(arguments, " ");
        return args;
    }

}
