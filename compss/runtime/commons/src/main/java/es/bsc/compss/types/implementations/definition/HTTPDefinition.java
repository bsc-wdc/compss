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

    public static final int NUM_PARAMS = 6;

    private String serviceName;
    private String resource;
    private String request;
    private String payload;
    private String payloadType;
    private String produces;


    public HTTPDefinition() {
        // For serialization
    }

    /**
     * Creates a new HTTPDefinition to create an HTTP core element implementation.
     *
     * @param request HTTP request type.
     * @param resource HTTP resource in the URL .
     */
    public HTTPDefinition(String serviceName, String resource, String request, String payload, String payloadType,
        String produces) {
        this.serviceName = serviceName;
        this.resource = resource;
        this.request = request;
        this.payload = payload;
        this.payloadType = payloadType;
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
        this.resource = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.request = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.payload = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
        this.payloadType = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
        this.produces = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 5]);
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getRequest() {
        return request;
    }

    public String getResource() {
        return resource;
    }

    public String getPayload() {
        return payload;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public String getProduces() {
        return produces;
    }

    @Override
    public String toString() {
        return "HTTP Definition \n" + "\t ServiceName: " + serviceName + "\n" + "\t HTTP resource: " + resource + "\n";
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.HTTP;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.request = (String) in.readObject();
        this.resource = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.request);
        out.writeObject(this.resource);
    }

    @Override
    public String toShortFormat() {
        return " HTTP method type: " + this.request + " with base URL: " + this.resource;
    }
}
