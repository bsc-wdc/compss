package integratedtoolkit.connectors.rocci.types.json;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class Attributes {

    @Expose
    private Occi occi;
    @Expose
    private Org org;

    public Occi getOcci() {
        return occi;
    }

    public void setOcci(Occi occi) {
        this.occi = occi;
    }

    public Org getOrg() {
        return org;
    }

    public void setOrg(Org org) {
        this.org = org;
    }

}
