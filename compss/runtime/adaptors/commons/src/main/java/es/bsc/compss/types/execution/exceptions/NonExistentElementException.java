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
package es.bsc.compss.types.execution.exceptions;

import java.util.LinkedList;
import java.util.List;


public class NonExistentElementException extends NonExistentDataException {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;

    private List<NonExistentDataException> subExceptions = new LinkedList<>();


    public NonExistentElementException(String dataName, List<NonExistentDataException> subExceptions) {
        super(dataName);
        this.subExceptions = subExceptions;
    }

    @Override
    public List<String> getMissingData() {
        List<String> missingData = new LinkedList<>();
        for (NonExistentDataException e : this.subExceptions) {
            missingData.addAll(e.getMissingData());
        }
        return missingData;
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder("Data " + dataName + "'s subelements [");
        List<String> subelementsAsList = new LinkedList<>();
        for (NonExistentDataException e : this.subExceptions) {
            subelementsAsList.add(String.join(",", e.getMissingData()));
        }
        sb.append(String.join(",", subelementsAsList));
        sb.append("] do not exists.");
        return sb.toString();

    }

}
