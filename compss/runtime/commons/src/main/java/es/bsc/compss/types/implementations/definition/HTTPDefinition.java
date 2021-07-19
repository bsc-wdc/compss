package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class HTTPDefinition implements ImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 4;

    private String methodType;
    private String baseUrl;
    private String jsonPayload;
    private String produces;


    public HTTPDefinition() {
        // For serialization
    }

    /**
     * Creates a new ServiceDefinition to create a HTTP core element implementation.
     *
     * @param methodType HTTP method type.
     * @param baseUrl HTTP bae URL .
     */
    public HTTPDefinition(String methodType, String baseUrl, String jsonPayload, String produces) {
        this.methodType = methodType;
        this.baseUrl = baseUrl;
        this.jsonPayload = jsonPayload;
        this.produces = produces;
    }

    /**
     * Creates a new Definition from string array.
     *
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public HTTPDefinition(String[] implTypeArgs, int offset) {
        this.methodType = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.baseUrl = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.jsonPayload = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.produces = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
    }

    public String getMethodType() {
        return methodType;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getJsonPayload() {
        return jsonPayload;
    }

    public String getProduces() {
        return produces;
    }

    @Override
    public String toString() {
        return "HTTP Definition \n" + "\t MethodType: " + methodType + "\n" + "\t HTTP base URL: " + baseUrl + "\n";
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.HTTP;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.methodType = (String) in.readObject();
        this.baseUrl = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.methodType);
        out.writeObject(this.baseUrl);
    }

    @Override
    public String toShortFormat() {
        return " HTTP method type: " + this.methodType + " with base URL: " + this.baseUrl;
    }
}
