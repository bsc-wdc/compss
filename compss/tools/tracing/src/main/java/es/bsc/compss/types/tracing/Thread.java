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

package es.bsc.compss.types.tracing;

import java.util.LinkedList;
import java.util.List;


public class Thread<I extends ThreadIdentifier> implements ApplicationStructure {

    private I identifier;
    private String label;


    public Thread(I identifier, String label) {
        this.identifier = identifier;
        this.label = label;
    }

    public void setIdentifier(I identifier) {
        this.identifier = identifier;
    }

    public I getIdentifier() {
        return identifier;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public int getNumberOfDirectSubcomponents() {
        return 0;
    }

    @Override
    public int getNumberOfTotalLowestSubcomponents() {
        return 1;
    }

    @Override
    public List<String> getAllLabels() {
        List<String> labels = new LinkedList<>();
        labels.add(label);
        return labels;
    }

    @Override
    public void print(String pad) {
        System.out.println(pad + " " + this.label);
    }

}
