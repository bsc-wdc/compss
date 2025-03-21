/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.util.test;

import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.AbstractMethodImplementationDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.serializers.Serializer;

import java.io.IOException;

import org.junit.Test;


public class ImplementationSerialization {

    @Test
    public void testSerializeMethodImplementation() throws ClassNotFoundException, IOException {
        MethodDefinition md = new MethodDefinition("class", "method");
        MethodDefinition md2 = (MethodDefinition) Serializer.deserialize(Serializer.serialize(md));
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> id =
            new ImplementationDescription<>(md, "signature", false, new MethodResourceDescription(), null, null);
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> id2 =
            (ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition>) Serializer
                .deserialize(Serializer.serialize(id));
        AbstractMethodImplementation ami = new AbstractMethodImplementation(0, 0, id);
        AbstractMethodImplementation ami2 =
            (AbstractMethodImplementation) Serializer.deserialize(Serializer.serialize(ami));

    }

}
