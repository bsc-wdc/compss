package es.bsc.compss.http.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


public class HTTPController {

    /**
     * Perform HTTP request.
     * @param methodType the HTTP method type: "GET", "POST", "PUT", "DELETE", etc.
     */
    public static Response performRequest(String methodType, String fullUrl) throws IOException {
        URL url = new URL(fullUrl);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(methodType);

        final int responseCode = connection.getResponseCode();
        final String responseBody = getResponseBody(connection);

        return new Response(responseCode, responseBody);
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
