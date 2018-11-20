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
package es.bsc.compss.invokers.external;

import java.util.LinkedList;
import java.util.List;


/**
 *
 * @author flordan
 */
public interface ExternalCommand {

    public static final String EXECUTE_TASK = "task";
    public static final String END_TASK = "endTask";
    public static final String ERROR_TASK = "errorTask";
    public static final String QUIT = "quit";
    public static final String REMOVE = "remove";
    public static final String SERIALIZE = "serialize";

    public static final String TOKEN_SEP = " ";

    public String getType();

    public String getAsString();


    public static class ExecuteTaskExternalCommand implements ExternalCommand {

        protected final LinkedList<String> arguments = new LinkedList<>();

        @Override
        public String getType() {
            return EXECUTE_TASK;
        }

        public void appendHeadArgument(String argument) {
            this.arguments.addFirst(argument);
        }

        public void appendTailArgument(String argument) {
            this.arguments.add(argument);
        }

        public void appendAllTail(List<String> arguments) {
            this.arguments.addAll(arguments);
        }

        @Override
        public String getAsString() {

            StringBuilder sb = new StringBuilder(EXECUTE_TASK);
            for (String c : arguments) {
                sb.append(TOKEN_SEP);
                sb.append(c);
            }
            return sb.toString();
        }
    }


    public static class EndTaskExternalCommand implements ExternalCommand {

        @Override
        public String getType() {
            return END_TASK;
        }

        @Override
        public String getAsString() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }


    public static class ErrorTaskExternalCommand implements ExternalCommand {

        @Override
        public String getType() {
            return ERROR_TASK;
        }

        @Override
        public String getAsString() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }


    public static class QuitExternalCommand implements ExternalCommand {

        @Override
        public String getType() {
            return QUIT;
        }

        @Override
        public String getAsString() {
            return QUIT;
        }

    }
}
