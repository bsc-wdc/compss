package es.bsc.compss.types.parameter;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import java.util.List;

/**
 * The internal Collection representation. A Collection is a COMPSs Parameter objects which may contain other
 * COMPSs parameter objects.
 * The object has an identifier by itself and points to other object identifiers (which are the ones contained in it)
 */
public class CollectionParameter extends DependencyParameter {

    // Identifier of the collection object
    private String collectionId;
    // Parameter objects of the collection contents
    private List< Parameter > parameters;

    /**
     * Default constructor. Intended to be called from COMPSsRuntimeImpl when gathering and compacting parameter
     * information fed from bindings or Java Loader
     * @param collectionFile Name of the File identifier of the collection object per se.
     * @param parameters Parameters of the CollectionParameter
     * @param direction Direction of the collection
     * @param stream N/A (At least temporarily)
     * @param prefix N/A (At least temporarily)
     * @param name Name of the parameter in the user code
     * @see DependencyParameter
     * @see es.bsc.compss.api.impl.COMPSsRuntimeImpl
     * @see es.bsc.compss.components.impl.TaskAnalyser
     */
    public CollectionParameter(String collectionFile, List< Parameter > parameters,
                               Direction direction, Stream stream, String prefix, String name) {
        // Type will always be COLLECTION_T, no need to pass it as a constructor parameter and wont be modified
        // Stream and prefix are still forwarded for possible, future uses
        super(DataType.COLLECTION_T, direction, stream, prefix, name);
        this.parameters = parameters;
        this.collectionId = collectionFile;
    }

    /**
     * Get the identifier of the collection
     * @return String
     */
    public String getCollectionId() {
        return collectionId;
    }

    /**
     * Set the identifier of the collection
     * @param collectionId String
     */
    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
    }

    /**
     * Representable print format of the collection
     * @return String
     */
    @Override
    public String toString() {
        // Stringbuilder adds less overhead when creating a string
        StringBuilder sb = new StringBuilder();
        sb.append("Collection ").append(collectionId).append("\n");
        sb.append("Name: ").append(getName()).append("\n");
        sb.append("Contents:\n");
        for(Parameter s : parameters) {
            sb.append("\t").append(s).append("\n");
        }
        return sb.toString();
    }

    /**
     *
     * @return List of Parameter
     */
    public List<Parameter> getParameters() {
        return parameters;
    }

    /**
     *
     * @param parameters List of Parameter
     */
    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }
}
