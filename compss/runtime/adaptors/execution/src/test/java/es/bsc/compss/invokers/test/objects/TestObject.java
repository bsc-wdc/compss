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
package es.bsc.compss.invokers.test.objects;

public class TestObject implements Comparable<TestObject> {

    private int value;


    public TestObject(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void updateValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TestObject(" + value + ")";
    }

    @Override
    public int compareTo(TestObject t) {
        return Integer.compare(value, t.value);
    }

    @Override
    public boolean equals(Object t) {
        if (t instanceof TestObject) {
            return Integer.compare(value, ((TestObject) t).value) == 0;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + this.value;
        return hash;
    }
}
