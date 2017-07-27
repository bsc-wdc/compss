package es.bsc.compss.ui;

public class StatisticParameter {

    private String name;
    private String value;


    public StatisticParameter() {
        this.setName(""); // Any
        this.setValue(""); // Any
    }

    public StatisticParameter(String name, String value) {
        this.setName(name);
        this.setValue(value);
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

}
