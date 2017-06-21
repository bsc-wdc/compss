package integratedtoolkit.types.resources.description;

import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.resources.MethodResourceDescription;

public class CloudMethodResourceDescription extends MethodResourceDescription {

    public static final CloudMethodResourceDescription EMPTY = new CloudMethodResourceDescription();

    // Resource Description
    private String providerName = "";
    private String name = "";
    private String type = "";
    private CloudImageDescription image = null;

    public CloudMethodResourceDescription() {
        super();
    }

    public CloudMethodResourceDescription(String providerName, String name, String type) {
        super();
        this.providerName = providerName;
        this.name = name;
        this.type = type;
    }

    public CloudMethodResourceDescription(Constraints constraints) {
        super(constraints);
    }

    public CloudMethodResourceDescription(MethodResourceDescription constraints) {
        super(constraints);
    }

    public CloudMethodResourceDescription(CloudMethodResourceDescription clone) {
        super(clone);
        providerName = clone.providerName;
        name = clone.name;
        type = clone.type;
        image = clone.image;
    }

    @Override
    public CloudMethodResourceDescription copy() {
        return new CloudMethodResourceDescription(this);
    }

    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public CloudImageDescription getImage() {
        return image;
    }

    public void setImage(CloudImageDescription image) {
        this.image = image;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("[CLOUD");
        sb.append(" PROVIDER =").append(this.providerName);
        sb.append(" IMAGE=").append((this.image == null) ? "NULL" : this.image.getImageName());
        sb.append(" TYPE=").append(this.type);
        sb.append("]");

        return sb.toString();
    }

}
