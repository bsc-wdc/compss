package integratedtoolkit.connectors.rocci.types.json;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class Attributes_ {

    @Expose
    private Occi_ occi;
    @Expose
    private Org_ org;

    public Occi_ getOcci() {
        return occi;
    }

    public void setOcci(Occi_ occi) {
        this.occi = occi;
    }

    public Org_ getOrg() {
        return org;
    }

    public void setOrg(Org_ org) {
        this.org = org;
    }

}
