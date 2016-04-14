package integratedtoolkit.connectors.rocci.types.json;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class Compute_ {

    @Expose
    private String id;
    @Expose
    private Float cpu;
    @Expose
    private String boot;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Float getCpu() {
        return cpu;
    }

    public void setCpu(Float cpu) {
        this.cpu = cpu;
    }

    public String getBoot() {
        return boot;
    }

    public void setBoot(String boot) {
        this.boot = boot;
    }

}
