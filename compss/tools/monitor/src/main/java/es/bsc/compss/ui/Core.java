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
package es.bsc.compss.ui;

import java.io.File;


public class Core {

    private String color;
    private String coreId;
    private String implId;
    private String signature;
    private String meanExecTime;
    private String minExecTime;
    private String maxExecTime;
    private String executedCount;


    /**
     * Creates a new empty CoreElement.
     */
    public Core() {
        // Empty Image
        this.setColor(File.separator + "images" + File.separator + "colors" + File.separator
            + Constants.CORE_COLOR_DEFAULT + ".png");

        this.setCoreId("0");
        this.setImplId("0");
        this.setSignature(""); // Any
        this.setMeanExecTime("0.0"); // Float
        this.setMinExecTime("0.0"); // Float
        this.setMaxExecTime("0.0"); // Float
        this.setExecutedCount("0"); // Int
    }

    /**
     * Creates a new CoreElement with the given {@code color} and information {@code info}.
     * 
     * @param color Display color of the core element.
     * @param info Internal information of the core element: coreId, implId, signature, mean/min/max times, and number
     *            of executions.
     */
    public Core(String color, String[] info) {
        /*
         * Each entry on the info array is of the form: coreId, implId, signature, meanET, minET, maxET, execCount
         */
        this.setColor(color);
        this.setCoreId(info[0]);
        this.setImplId(info[1]);
        this.setSignature(info[2]);
        this.setMeanExecTime(info[3]);
        this.setMinExecTime(info[4]);
        this.setMaxExecTime(info[5]);
        this.setExecutedCount(info[6]);
    }

    public String getColor() {
        return this.color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getCoreId() {
        return this.coreId;
    }

    public String getImplId() {
        return this.implId;
    }

    public String getSignature() {
        return this.signature;
    }

    public String getMeanExecTime() {
        return this.meanExecTime;
    }

    public String getMinExecTime() {
        return this.minExecTime;
    }

    public String getMaxExecTime() {
        return this.maxExecTime;
    }

    public void setCoreId(String coreId) {
        this.coreId = coreId;
    }

    public void setImplId(String implId) {
        this.implId = implId;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public void setMeanExecTime(String meanExecTime) {
        this.meanExecTime = meanExecTime;
    }

    public void setMinExecTime(String minExecTime) {
        this.minExecTime = minExecTime;
    }

    public void setMaxExecTime(String maxExecTime) {
        this.maxExecTime = maxExecTime;
    }

    public String getExecutedCount() {
        return this.executedCount;
    }

    public void setExecutedCount(String executedCount) {
        this.executedCount = executedCount;
    }

}
