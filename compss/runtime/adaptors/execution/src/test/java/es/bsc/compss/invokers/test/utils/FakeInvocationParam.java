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
package es.bsc.compss.invokers.test.utils;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;

import java.util.LinkedList;
import java.util.List;


public class FakeInvocationParam implements InvocationParam {

    private DataType type;
    private String originalName;
    private String renamedName;
    private Object value;
    private Class<?> valueClass;

    private final String prefix;
    private final String name;
    private final String contentType;
    private final double weight;
    private final boolean keepRename;
    private final StdIOStream stream;
    private final boolean writeFinalValue;
    private final String dataMgmtId;


    /**
     * Fake Invocation parameter constructor.
     * 
     * @param type Type
     * @param prefix Prefix
     * @param name Name
     * @param contentType Python Object Type
     * @param stream Stream
     * @param originalName Original Name
     * @param dataMgmtId Data Management Id
     * @param writeFinalValue Write final value flag
     */
    public FakeInvocationParam(DataType type, String prefix, String name, String contentType, StdIOStream stream,
        double weight, boolean keepRename, String originalName, String dataMgmtId, boolean writeFinalValue) {
        this.type = type;
        this.prefix = prefix;
        this.name = name;
        this.contentType = contentType;
        this.weight = weight;
        this.keepRename = keepRename;
        this.stream = stream;
        this.originalName = originalName;
        this.writeFinalValue = writeFinalValue;
        this.dataMgmtId = dataMgmtId;
    }

    @Override
    public void setType(DataType type) {
        this.type = type;
    }

    @Override
    public DataType getType() {
        return this.type;
    }

    @Override
    public boolean isCollective() {
        return false;
    }

    @Override
    public boolean isPreserveSourceData() {
        return false;
    }

    @Override
    public boolean isWriteFinalValue() {
        return writeFinalValue;
    }

    @Override
    public String getPrefix() {
        return this.prefix;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getContentType() {
        return this.contentType;
    }

    @Override
    public StdIOStream getStdIOStream() {
        return this.stream;
    }

    @Override
    public double getWeight() {
        return this.weight;
    }

    @Override
    public boolean isKeepRename() {
        return this.keepRename;
    }

    @Override
    public String getOriginalName() {
        return this.originalName;
    }

    @Override
    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    @Override
    public String getRenamedName() {
        return this.renamedName;
    }

    @Override
    public void setRenamedName(String renamedName) {
        this.renamedName = renamedName;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public void setValue(Object val) {
        this.value = val;
    }

    @Override
    public void setValueClass(Class<?> valueClass) {
        this.valueClass = valueClass;
    }

    @Override
    public Class<?> getValueClass() {
        return this.valueClass;
    }

    @Override
    public String getDataMgmtId() {
        return this.dataMgmtId;
    }

    @Override
    public String getSourceDataId() {
        return null;
    }

    @Override
    public List<InvocationParamURI> getSources() {
        // No resource where to get the data. Value is automatically provided
        return new LinkedList<>();
    }

    @Override
    public void resultIsForwarded() {
        // Do nothing
    }

    @Override
    public boolean isForwardedResult() {
        return false;
    }
}
