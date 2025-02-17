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


public class ApplicationComposition<T extends ApplicationStructure> implements ApplicationStructure {

    private final List<T> components;


    public ApplicationComposition() {
        this.components = new LinkedList<>();
    }

    public void appendComponent(T c) {
        this.components.add(c);
    }

    /**
     * Replaces the components of the composition.
     * 
     * @param subComponents new subcomponents
     */
    public void replaceSubcomponents(List<T> subComponents) {
        components.clear();
        components.addAll(subComponents);
    }

    public List<T> getSubComponents() {
        return this.components;
    }

    @Override
    public int getNumberOfDirectSubcomponents() {
        return this.components.size();
    }

    @Override
    public int getNumberOfTotalLowestSubcomponents() {
        int total = 0;
        for (ApplicationStructure component : components) {
            total += component.getNumberOfTotalLowestSubcomponents();
        }
        return total;
    }

    @Override
    public List<String> getAllLabels() {
        List<String> labels = new LinkedList<>();
        for (ApplicationStructure component : components) {
            labels.addAll(component.getAllLabels());
        }
        return labels;
    }

    @Override
    public void print(String pad) {
        System.out.println(pad + "-");
        for (ApplicationStructure component : this.components) {
            component.print(pad + "\t");
        }
    }

}
