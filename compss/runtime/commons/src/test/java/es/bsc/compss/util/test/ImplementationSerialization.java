package es.bsc.compss.util.test;

import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.AbstractMethodImplementationDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.Serializer;

import java.io.IOException;

import org.junit.Test;


public class ImplementationSerialization {

    @Test
    public void testSerializeMethodImplementation() throws ClassNotFoundException, IOException {
        MethodDefinition md = new MethodDefinition("class", "method");
        MethodDefinition md2 = (MethodDefinition) Serializer.deserialize(Serializer.serialize(md));
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> id =
            new ImplementationDescription<>(md, "signature", new MethodResourceDescription());
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> id2 =
            (ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition>) Serializer
                .deserialize(Serializer.serialize(id));
        AbstractMethodImplementation ami = new AbstractMethodImplementation(0, 0, id);
        AbstractMethodImplementation ami2 =
            (AbstractMethodImplementation) Serializer.deserialize(Serializer.serialize(ami));

    }

}
