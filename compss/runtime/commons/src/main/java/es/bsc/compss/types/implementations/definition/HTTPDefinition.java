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

    public static final int NUM_PARAMS = 5;

    private String serviceName;
    private String methodType;
    private String baseUrl;
    private String jsonPayload;
    private String produces;


    public HTTPDefinition() {
        // For serialization
    }

    /**
     * Creates a new HTTPDefinition to create an HTTP core element implementation.
     *
     * @param methodType HTTP method type.
     * @param baseUrl HTTP base URL .
     */
    public HTTPDefinition(String serviceName, String baseUrl, String methodType, String jsonPayload, String produces) {
        this.serviceName = serviceName;
        this.baseUrl = baseUrl;
        this.methodType = methodType;
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
        this.serviceName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.baseUrl = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.methodType = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.jsonPayload = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
        this.produces = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
    }

    public String getServiceName() {
        return serviceName;
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
        return "HTTP Definition \n" + "\t ServiceName: " + serviceName + "\n" + "\t HTTP base URL: " + baseUrl + "\n";
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
