package integratedtoolkit.connectors.rocci.types.json;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class Occi_ {

    @Expose
    private Core_ core;
    @Expose
    private Networkinterface networkinterface;

    public Core_ getCore() {
        return core;
    }

    public void setCore(Core_ core) {
        this.core = core;
    }

    public Networkinterface getNetworkinterface() {
        return networkinterface;
    }

    public void setNetworkinterface(Networkinterface networkinterface) {
        this.networkinterface = networkinterface;
    }

}
