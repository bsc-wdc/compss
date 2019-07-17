/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.definition.ImplementationDefinition;


public class FakeImplDefinition extends ImplementationDefinition<FakeResourceDescription> {

    public FakeImplDefinition(String signature, FakeResourceDescription desc) {
        super(signature, desc);
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new FakeImplementation(coreId, implId, this.getSignature(), this.getConstraints());
    }
}
