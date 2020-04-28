package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.ContainerImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.MethodResourceDescription;


/**
 * Class containing all the necessary information to generate a Container implementation of a CE.
 */
public class ContainerDefinition extends ImplementationDefinition<MethodResourceDescription> {

    private final String engine;
    private final String image;
    private final String binary;


    protected ContainerDefinition(String signature, String engine, String image, String binary,
        MethodResourceDescription constraints) {
        super(signature, constraints);
        this.engine = engine;
        this.image = image;
        this.binary = binary;
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new ContainerImplementation(engine, image, binary, coreId, implId, this.getSignature(),
            this.getConstraints());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("COntainer Implementation \n");
        sb.append("\t Signature: ").append(this.getSignature()).append("\n");
        sb.append("\t Engine: ").append(engine).append("\n");
        sb.append("\t Imagen: ").append(image).append("\n");
        sb.append("\t Binary: ").append(binary).append("\n");
        sb.append("\t Constraints: ").append(this.getConstraints());
        return sb.toString();
    }

}
