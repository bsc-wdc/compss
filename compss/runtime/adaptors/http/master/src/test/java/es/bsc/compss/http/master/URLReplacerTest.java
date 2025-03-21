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
package es.bsc.compss.http.master;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;


public class URLReplacerTest {

    @Test
    public void testReplaceUrlParametersWithNullMapShouldReturnOriginalUrl() {
        String baseUrl = "https://webpage.com";

        String expected = "https://webpage.com";
        String actual = URLReplacer.replaceUrlParameters(baseUrl, null, "\\{", "\\}");
        assertEquals(expected, actual);
    }

    @Test
    public void testReplaceUrlParametersWithEmptyMapShouldReturnOriginalUrl() {
        String baseUrl = "https://webpage.com";

        String expected = "https://webpage.com";
        String actual = URLReplacer.replaceUrlParameters(baseUrl, new HashMap<>(), "\\{", "\\}");
        assertEquals(expected, actual);
    }

    @Test
    public void testReplaceUrlParametersWhithOneTokenToReplace() {
        String baseUrl = "https://ljx0ny586l.execute-api.us-east-1.amazonaws.com/dev/api/array?size={size}";

        Map<String, String> replaceElements = new HashMap<>();
        replaceElements.put("size", "4");

        String expected = "https://ljx0ny586l.execute-api.us-east-1.amazonaws.com/dev/api/array?size=4";
        String actual = URLReplacer.replaceUrlParameters(baseUrl, replaceElements, "\\{", "\\}");
        assertEquals(expected, actual);
    }

    @Test
    public void testReplaceUrlParametersWhithFiveTokenToReplace() {
        String baseUrl = "https://aldo_fuster_turpin.com/{id}/{a}?b={b}&c={c}";

        Map<String, String> replaceElements = new HashMap<>();
        replaceElements.put("id", "id_value");
        replaceElements.put("a", "a_value");
        replaceElements.put("b", "b_value");
        replaceElements.put("c", "c_value");

        String expected = "https://aldo_fuster_turpin.com/id_value/a_value?b=b_value&c=c_value";
        String actual = URLReplacer.replaceUrlParameters(baseUrl, replaceElements, "\\{", "\\}");
        assertEquals(expected, actual);
    }
}
