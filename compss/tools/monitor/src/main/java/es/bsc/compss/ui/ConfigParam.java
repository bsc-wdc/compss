/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.ui;

public class ConfigParam {

    private String name;
    private String value;
    private boolean editingStatus;


    public ConfigParam() {
        this.setName(""); // Any
        this.setValue(""); // Any
        this.setEditingStatus(false);
    }

    public ConfigParam(String name, String value, boolean editingStatus) {
        this.setName(name);
        this.setValue(value);
        this.setEditingStatus(editingStatus);
    }

    public ConfigParam(ConfigParam cp) {
        this.setName(cp.name);
        this.setValue(cp.value);
        this.setEditingStatus(cp.editingStatus);
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean getEditingStatus() {
        return this.editingStatus;
    }

    public void setEditingStatus(boolean editingStatus) {
        this.editingStatus = editingStatus;
    }

    public void update() {

    }
}
