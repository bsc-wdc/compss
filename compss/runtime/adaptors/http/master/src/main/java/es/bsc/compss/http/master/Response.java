package es.bsc.compss.http.master;

public class Response {

    private final int responseCode;
    private final String responseBody;


    public Response(int responseCode, String responseBody) {
        this.responseCode = responseCode;
        this.responseBody = responseBody;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getResponseBody() {
        return responseBody;
    }
}
