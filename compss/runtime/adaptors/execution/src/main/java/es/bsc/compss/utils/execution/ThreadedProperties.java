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

package es.bsc.compss.utils.execution;

import java.util.Properties;


/**
 * Utility to allow different properties for each thread.
 */
public class ThreadedProperties extends Properties {

    // Custom Serial ID.
    private static final long serialVersionUID = 3L;

    private final ThreadLocal<Properties> localProperties = new ThreadLocal<Properties>() {

        @Override
        protected Properties initialValue() {
            return new Properties();
        }
    };


    /**
     * Constructs a new ThreadedProperties using the properties passed in as parameter as parameter.
     *
     * @param properties Properties shared by all threads
     */
    public ThreadedProperties(Properties properties) {
        super(properties);
    }

    @Override
    public String getProperty(String key) {
        String localValue = this.localProperties.get().getProperty(key);
        if (localValue != null) {
            return localValue;
        }

        return super.getProperty(key);
    }

    @Override
    public Object setProperty(String key, String value) {
        return this.localProperties.get().setProperty(key, value);
    }
}
