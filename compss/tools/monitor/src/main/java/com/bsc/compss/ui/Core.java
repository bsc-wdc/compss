package com.bsc.compss.ui;

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


    public Core() {
        // Empty Image
        this.setColor(File.separator + "images" + File.separator + "colors" + File.separator + Constants.CORE_COLOR_DEFAULT + ".png");

        this.setCoreId("0");
        this.setImplId("0");
        this.setSignature(""); // Any
        this.setMeanExecTime("0.0"); // Float
        this.setMinExecTime("0.0"); // Float
        this.setMaxExecTime("0.0"); // Float
        this.setExecutedCount("0"); // Int
    }

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
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getCoreId() {
        return coreId;
    }

    public String getImplId() {
        return implId;
    }

    public String getSignature() {
        return signature;
    }

    public String getMeanExecTime() {
        return meanExecTime;
    }

    public String getMinExecTime() {
        return minExecTime;
    }

    public String getMaxExecTime() {
        return maxExecTime;
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
        return executedCount;
    }

    public void setExecutedCount(String executedCount) {
        this.executedCount = executedCount;
    }

}
