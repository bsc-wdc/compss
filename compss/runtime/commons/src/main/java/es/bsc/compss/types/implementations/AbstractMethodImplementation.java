package es.bsc.compss.types.implementations;

import es.bsc.compss.types.implementations.definition.AbstractMethodImplementationDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;


public class AbstractMethodImplementation extends Implementation {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Generate a dummy method implementation.
     * 
     * @param constraints Method resource description for the dummy implementation
     * @return Dummy abstract method implementation.
     */
    public static AbstractMethodImplementation generateDummy(MethodResourceDescription constraints) {
        return new AbstractMethodImplementation(null, null,
            new ImplementationDescription<>(new MethodDefinition("", ""), "", constraints));
    }

    public AbstractMethodImplementation() {
        // For serialization
        super();
    }

    public AbstractMethodImplementation(Integer coreId, Integer implId,
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> implDesc) {
        super(coreId, implId, implDesc);
    }

    @Override
    public ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition>
        getDescription() {
        return (ImplementationDescription<MethodResourceDescription,
            AbstractMethodImplementationDefinition>) this.implDescription;

    }

    @Override
    public MethodResourceDescription getRequirements() {
        return this.getDescription().getConstraints();

    }

    public AbstractMethodImplementationDefinition getDefinition() {
        return getDescription().getDefinition();
    }

    public String getMethodDefinition() {
        return getDescription().getDefinition().toMethodDefinitionFormat();
    }

    public MethodType getMethodType() {
        return getDescription().getDefinition().getMethodType();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
