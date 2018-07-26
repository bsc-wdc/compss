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
package es.bsc.compss.invokers.test.utils;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import java.util.LinkedList;
import java.util.List;


/**
 *
 * @author flordan
 */
public class FakeInvocationParam implements InvocationParam {

    private DataType type;
    private String originalName;
    private Object value;
    private Class<?> valueClass;

    private final String prefix;
    private final Stream stream;
    private final boolean writeFinalValue;
    private final String dataMgmtId;

    public FakeInvocationParam(DataType type, String prefix, Stream stream, String originalName, String dataMgmtId, boolean writeFinalValue) {
        this.type = type;
        this.prefix = prefix;
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
    public Stream getStream() {
        return this.stream;
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
    public List<InvocationParamURI> getSources() {
        //No resource where to get the data. Value is automatically provided
        return new LinkedList<>();
    }
}
