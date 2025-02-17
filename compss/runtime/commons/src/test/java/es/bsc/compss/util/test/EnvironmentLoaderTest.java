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
package es.bsc.compss.util.test;

import static org.junit.Assert.assertEquals;

import es.bsc.compss.util.EnvironmentLoader;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;


/**
 * Tests the different possibilities of loading a environment variable.
 */
public class EnvironmentLoaderTest {

    private static final String VALUE1 = "1";
    private static final String VALUE2 = "2";
    private static final String PATH1 = "/tmp/";
    private static final String PATH2 = "try/";

    @ClassRule
    public static final EnvironmentVariables ENVIRONMENT_VARIABLES = new EnvironmentVariables();


    /**
     * Setup environment before tests execution.
     * 
     * @throws Exception Error when environment cannot be setup.
     */
    @Before
    public void setUp() throws Exception {
        ENVIRONMENT_VARIABLES.set("var", VALUE1);
        ENVIRONMENT_VARIABLES.set("var1", VALUE1);
        ENVIRONMENT_VARIABLES.set("var2", VALUE2);
    }

    @Test
    public void noEnv() {
        String expression = "1";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, VALUE1);
    }

    @Test
    public void simpleEnv1() {
        String expression = "$var";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, VALUE1);
    }

    @Test
    public void simpleEnv2() {
        String expression = "${var}";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, VALUE1);
    }

    @Test
    public void complexEnv1() {
        String expression = "$var/tmp/try/";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, VALUE1 + PATH1 + PATH2);
    }

    @Test
    public void complexEnv2() {
        String expression = "${var}/tmp/try/";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, VALUE1 + PATH1 + PATH2);
    }

    @Test
    public void complexEnv3() {
        String expression = "/tmp/try/$var";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + PATH2 + VALUE1);
    }

    @Test
    public void complexEnv4() {
        String expression = "/tmp/try/${var}";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + PATH2 + VALUE1);
    }

    @Test
    public void complexEnv5() {
        String expression = "/tmp/$var/try/";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + VALUE1 + "/" + PATH2);
    }

    @Test
    public void complexEnv6() {
        String expression = "/tmp/${var}/try/";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + VALUE1 + "/" + PATH2);
    }

    @Test
    public void doubleEnv1() {
        String expression = "/tmp/$var1/$var2/try/";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + VALUE1 + "/" + VALUE2 + "/" + PATH2);
    }

    @Test
    public void doubleEnv2() {
        String expression = "/tmp/${var1}/$var2/try/";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + VALUE1 + "/" + VALUE2 + "/" + PATH2);
    }

    @Test
    public void doubleEnv3() {
        String expression = "/tmp/${var1}/${var2}/try/";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + VALUE1 + "/" + VALUE2 + "/" + PATH2);
    }

    @Test
    public void doubleEnv4() {
        String expression = "$var1/${var2}";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, VALUE1 + "/" + VALUE2);
    }

    @Test
    public void doubleEnv5() {
        String expression = "${var1}/$var2";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, VALUE1 + "/" + VALUE2);
    }

    @Test
    public void scapedEnvVar1() {
        String expression = "\\${var}";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, "${var}");
    }

    @Test
    public void scapedEnvVar2() {
        String expression = "\\$var";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, "$var");
    }

    @Test
    public void scapedEnvVar3() {
        String expression = "/tmp/\\${var}";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + "${var}");
    }

    @Test
    public void scapedEnvVar4() {
        String expression = "/tmp/${var1}/\\${var}/$var2";
        String expressionValue = EnvironmentLoader.loadFromEnvironment(expression);

        assertEquals(expressionValue, PATH1 + VALUE1 + "/${var}/" + VALUE2);
    }

}
