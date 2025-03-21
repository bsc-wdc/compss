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

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;


public class HTTPController {

    /**
     * Perform HTTP request.
     *
     * @param request the HTTP method type: "GET", "POST", "PUT", "DELETE", etc.
     * @param fullUrl the full target URL the HTTP request
     */
    public static Response performRequestAndGetResponse(String request, String fullUrl, String payload,
        String payloadType) throws IOException {
        URL url = new URL(fullUrl);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(request);

        // todo: switch to switch
        if (request.equals("POST")) {
            performPostRequest(connection, payload, payloadType);
        }

        final int responseCode = connection.getResponseCode();
        final String responseBody = getResponseBody(connection);

        boolean isJson = false;

        // make sure that the response is in JSON format
        Map<String, List<String>> headers = connection.getHeaderFields();
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equals("Content-Type") && entry.getValue() != null
                && entry.getValue().get(0) != null && entry.getValue().get(0).equals("application/json")) {
                isJson = true;
                break;
            }
        }

        String jsonBody;
        if (!isJson) {
            Gson gson = new Gson();
            jsonBody = gson.toJson(responseBody);
        } else {
            jsonBody = responseBody;
        }

        return new Response(responseCode, jsonBody);
    }

    private static void performPostRequest(HttpURLConnection connection, String payload, String payloadType)
        throws IOException {

        connection.setDoOutput(true);
        // todo: accept only json?
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", payloadType);

        // todo: is it possible to send json file without reading
        byte[] out = payload.getBytes();

        OutputStream stream = connection.getOutputStream();
        stream.write(out);

    }

    private static String getResponseBody(HttpURLConnection connection) throws IOException {
        final InputStream inputStream = connection.getInputStream();
        final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        StringBuilder stringBuffer = new StringBuilder();

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            stringBuffer.append(line);
        }
        bufferedReader.close();
        connection.disconnect();
        return stringBuffer.toString();
    }
}
