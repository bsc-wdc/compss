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
package es.bsc.compss.types.data.location;

import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;

import java.util.LinkedList;
import java.util.List;

import storage.StorageException;
import storage.StorageItf;


public class BindingObjectLocation extends DataLocation {

    private final String id;
    BindingObject bindingObject;
    private final MultiURI uri;

    public BindingObjectLocation(Resource host, BindingObject bo) {
        super(); 
        this.id = bo.getId();
        this.uri = new MultiURI(Protocol.BINDING_URI, host, id+"#"+bo.getType()+"#"+bo.getElements());
        this.bindingObject=bo;
    }

    @Override
    public DataLocation.Type getType() {
        return DataLocation.Type.BINDING;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.BINDING_URI;
    }

    public String getId() {
        return this.id;
    }
    
    public BindingObject getBindingObject(){
        return this.bindingObject;
    }

    @Override
    public List<MultiURI> getURIs() {
        List<MultiURI> list = new LinkedList<>();
        list.add(this.uri);

        return list;
    }

    @Override
    public List<Resource> getHosts() {
        List<Resource> list = new LinkedList<>();
        list.add(this.uri.getHost());
        return list;
    }

    @Override
    public MultiURI getURIInHost(Resource targetHost) {
        if (this.uri.getHost().equals(targetHost)) {
            return this.uri;
        } else {
            return null;
        }
    }
    
    @Override
    public boolean isTarget(DataLocation target) {
        if (target.getType() != DataLocation.Type.BINDING) {
            return false;
        }

        return this.id.equals(((BindingObjectLocation) target).id);
    }

    @Override
    public String getSharedDisk() {
        return null;
    }

    @Override
    public String getPath() {
        return this.uri.getPath();
    }

    @Override
    public String getLocationKey() {
        return this.uri.getPath() + ":" + this.uri.getHost().getName();
    }

    @Override
    public int compareTo(DataLocation o) {
        if (o == null) {
            throw new NullPointerException();
        }
        if (o.getClass() != BindingObjectLocation.class) {
            return (this.getClass().getName()).compareTo(BindingObjectLocation.class.toString());
        } else {
            return this.uri.compareTo(((BindingObjectLocation) o).uri);
        }
    }

    @Override
    public String toString() {
        return this.uri.toString();
    }
    

}
