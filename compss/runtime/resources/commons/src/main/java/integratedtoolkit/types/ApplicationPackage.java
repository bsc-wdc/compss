package integratedtoolkit.types;

public class ApplicationPackage {

    private final String source;
    private final String target;


    public ApplicationPackage(String source, String target) {
        this.source = source;
        this.target = target;
    }

    public String getSource() {
        return this.source;
    }

    public String getTarget() {
        return this.target;
    }

}
