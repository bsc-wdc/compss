package com.bsc.compss.ui;

public class Application {

    private String name;
    private String path;


    public Application() {
        this.setName(""); // Any
        this.setPath(""); // Any
    }

    public Application(String name, String path) {
        this.setName(name);
        this.setPath(path);
    }

    public Application(Application app) {
        this.setName(app.name);
        this.setPath(app.path);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
