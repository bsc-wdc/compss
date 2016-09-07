package com.bsc.compss.ui;

public class ConfigParam {

    private String name;
    private String value;
    private boolean editingStatus;


    public ConfigParam() {
        this.setName(""); // Any
        this.setValue(""); // Any
        this.setEditingStatus(false);
    }

    public ConfigParam(String name, String value, boolean editingStatus) {
        this.setName(name);
        this.setValue(value);
        this.setEditingStatus(editingStatus);
    }

    public ConfigParam(ConfigParam cp) {
        this.setName(cp.name);
        this.setValue(cp.value);
        this.setEditingStatus(cp.editingStatus);
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean getEditingStatus() {
        return this.editingStatus;
    }

    public void setEditingStatus(boolean editingStatus) {
        this.editingStatus = editingStatus;
    }

    public void update() {

    }
}
