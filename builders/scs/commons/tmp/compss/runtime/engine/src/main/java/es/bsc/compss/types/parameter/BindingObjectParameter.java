/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.parameter;

import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.parameter.DependencyParameter;


public class BindingObjectParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private final int hashCode;
    private final BindingObject bo;
 	
    public BindingObjectParameter(Direction direction, Stream stream, String prefix, String name, BindingObject bo, int hashCode) {
        super(DataType.BINDING_OBJECT_T, direction, stream, prefix, name);
        this.bo = bo;
        this.hashCode = hashCode;
    }

    public String getId() {
        return this.bo.toString();
    }

    public int getCode() {
        return this.hashCode;
    }
    
    @Override
    public String toString() {
        return "BindingObjectParameter with Id " + this.bo.getId() + ", type " + this.bo.getType() + 
        		", elements "+this.bo.getElements()+" and HashCode " + this.hashCode;
    }
    @Override
    public String getOriginalName(){
    	return this.bo.getId();
    }
	
	@Override
	public String getDataTarget(){
	    String dataTarget = super.getDataTarget();
	    if (dataTarget!=null){
	        if (dataTarget.contains("#")){
	            return dataTarget;
	        }else{
	            return dataTarget+"#"+bo.getType()+"#"+bo.getElements();
	        }
	    }else{
	        return "null#"+bo.getType()+"#"+bo.getElements();
	    }
	}

    public BindingObject getBindingObject() {
        return bo;
    }
		

}
