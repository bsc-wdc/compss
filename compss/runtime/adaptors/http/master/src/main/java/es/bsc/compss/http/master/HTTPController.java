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
     * @param methodType the HTTP method type: "GET", "POST", "PUT", "DELETE", etc.
     * @param fullUrl the full target URL the HTTP request
     */
    public static Response performRequestAndGetResponse(String methodType, String fullUrl, String jsonPayload)
        throws IOException {
        URL url = new URL(fullUrl);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(methodType);

        // todo: switch to switch
        if (methodType.equals("POST")) {
            performPostRequest(connection, jsonPayload);
        }

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

    private static void performPostRequest(HttpURLConnection connection, String data) throws IOException {

        connection.setDoOutput(true);
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");

        connection.setRequestProperty("size", "3");

        // todo: is it possible to send json file without reading
        byte[] out = data.getBytes();

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
