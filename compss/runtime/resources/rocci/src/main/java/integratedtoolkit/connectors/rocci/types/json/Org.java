package integratedtoolkit.connectors.rocci.types.json;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class Org {

    @Expose
    private Opennebula opennebula;
    @Expose
    private Openstack openstack;

    public Opennebula getOpennebula() {
        return opennebula;
    }

    public void setOpennebula(Opennebula opennebula) {
        this.opennebula = opennebula;
    }

    public Openstack getOpenstack() {
        return openstack;
    }

    public void setOpenstack(Openstack openstack) {
        this.openstack = openstack;
    }

}
