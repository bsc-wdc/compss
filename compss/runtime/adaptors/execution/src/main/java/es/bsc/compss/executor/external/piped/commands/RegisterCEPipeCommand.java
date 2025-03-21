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
package es.bsc.compss.executor.external.piped.commands;

import es.bsc.compss.executor.external.commands.RegisterCEExternalCommand;


/**
 * Command to request the registration of a new CE.
 */
public class RegisterCEPipeCommand extends RegisterCEExternalCommand implements PipeCommand {

    /**
     * Instantiates a new RegisterCE command parsing the command read through the pipe.
     * 
     * @param command pipe message
     */
    public RegisterCEPipeCommand(String[] command) {
        this.ceSignature = command[1];
        this.implSignature = command[2];
        this.constraints = command[3];
        this.implType = command[4];
        this.implLocal = command[5];
        this.implIO = command[6];

        this.prolog = new String[3];
        System.arraycopy(command, 7, this.prolog, 0, 3);
        this.epilog = new String[3];
        System.arraycopy(command, 10, this.epilog, 0, 3);

        this.container = new String[3];
        System.arraycopy(command, 13, this.container, 0, 3);

        int typeArgsLength = Integer.parseInt(command[16]);
        this.typeArgs = new String[typeArgsLength];
        System.arraycopy(command, 17, this.typeArgs, 0, typeArgsLength);
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value == 0) {
            RegisterCEPipeCommand rce = (RegisterCEPipeCommand) t;
            value = this.ceSignature.compareTo(rce.ceSignature);

            if (value == 0) {
                value = this.implSignature.compareTo(rce.implSignature);
            }
        }
        return value;
    }

}
