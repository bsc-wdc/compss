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
package es.bsc.compss.types.data.location;

import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;

import java.util.LinkedList;
import java.util.List;


public class PrivateLocation extends DataLocation {

    private MultiURI uri;


    public PrivateLocation(ProtocolType protocol, Resource host, String path) {
        super();
        this.uri = new MultiURI(protocol, host, path);
    }

    @Override
    public LocationType getType() {
        return LocationType.PRIVATE;
    }

    @Override
    public ProtocolType getProtocol() {
        return this.uri.getProtocol();
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
        if (this.uri.getHost().getNode().equals(targetHost.getNode())) {
            return this.uri;
        } else {
            return null;
        }
    }

    @Override
    public boolean isTarget(DataLocation target) {
        if (target.getType() != LocationType.PRIVATE) {
            return false;
        }
        MultiURI targetURI = ((PrivateLocation) target).uri;
        return (targetURI.getHost() == uri.getHost() && targetURI.getPath().contentEquals(uri.getPath()));
    }

    @Override
    public SharedDisk getSharedDisk() {
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
        if (o.getClass() != PrivateLocation.class) {
            return (this.getClass().getName()).compareTo(PrivateLocation.class.toString());
        } else {
            return this.uri.compareTo(((PrivateLocation) o).uri);
        }
    }

    @Override
    public String toString() {
        return this.uri.toString();
    }

    @Override
    public void modifyPath(String path) {
        this.uri = new MultiURI(this.uri.getProtocol(), this.uri.getHost(), path);

    }

}
