package es.bsc.compss.http.master;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;


public class HTTPController {

    /**
     * Perform HTTP request.
     *
     * @param methodType the HTTP method type: "GET", "POST", "PUT", "DELETE", etc.
     * @param fullUrl the full target URL of the HTTP request
     */
    public static Response performRequestAndGetResponse(String methodType, String fullUrl) throws IOException {
        URL url = new URL(fullUrl);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(methodType);

        final int responseCode = connection.getResponseCode();
        final String responseBody = getResponseBody(connection);

        boolean isJson = false;

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

        return stringBuffer.toString();
    }
}
