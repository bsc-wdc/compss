package integratedtoolkit.connectors.rocci.types.json;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class Credentials {

    @Expose
    private Publickey publickey;

    public Publickey getPublickey() {
        return publickey;
    }

    public void setPublickey(Publickey publickey) {
        this.publickey = publickey;
    }

}
