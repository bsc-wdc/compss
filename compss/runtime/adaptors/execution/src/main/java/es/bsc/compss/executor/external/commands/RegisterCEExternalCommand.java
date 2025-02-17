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
package es.bsc.compss.executor.external.commands;

/**
 * Command to request the registration of a new CE.
 */
public class RegisterCEExternalCommand implements ExternalCommand {

    protected String ceSignature;
    protected String implSignature;
    protected String constraints;
    protected String implType;
    protected String implLocal;
    protected String implIO;
    protected String[] typeArgs;
    protected String[] prolog;
    protected String[] epilog;

    protected String[] container;


    public String getCESignature() {
        return this.ceSignature;
    }

    public String getImplSignature() {
        return this.implSignature;
    }

    public String getConstraints() {
        return this.constraints;
    }

    public String getImplType() {
        return this.implType;
    }

    public String getImplLocal() {
        return this.implLocal;
    }

    public String getImplIO() {
        return this.implIO;
    }

    public String[] getTypeArgs() {
        return this.typeArgs;
    }

    public String[] getProlog() {
        return prolog;
    }

    public String[] getEpilog() {
        return epilog;
    }

    public String[] getConatainer() {
        return container;
    }

    @Override
    public CommandType getType() {
        return CommandType.REGISTER_CE;
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(CommandType.REGISTER_CE.name());
        return sb.toString();
    }

}
