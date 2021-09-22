package es.bsc;

import com.google.gson.Gson;
import org.eclipse.jetty.server.Server;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.json.JSONObject;


@Path("/test")
public class testREST {

    private int port;
    private Server server = null;


    public testREST() {

    }

    public testREST(int port) {
        this.port = port;
    }

    private synchronized void start() throws Exception {
        if (this.server != null) {
            // Server already started. Ignore start;
            return;
        }
        RESTServiceLauncher launcher = null;
        launcher = new RESTServiceLauncher(port);
        new Thread(launcher).start();
        launcher.waitForBoot();
        if (launcher.getStartError() != null) {
            throw new Exception(launcher.getStartError());
        } else {
            this.server = launcher.getServer();
        }
    }

    private synchronized void stop() {
        if (this.server != null) {
            new Thread() {

                @Override
                public void run() {
                    Thread.currentThread().setName("REST Agent Service Stopper");
                    try {
                        Thread.sleep(500);
                        testREST.this.server.stop();
                    } catch (Exception ex) {
                        if (!server.isStopped()) {
                            server.destroy();
                        }
                    } finally {
                        server = null;
                    }
                }
            }.start();

        }
    }

    @GET
    @Path("stop/")
    public Response shutDown() {
        stop();
        return respond("ok");
    }

    @GET
    @Path("dummy/")
    public Response dummy() {
        return stringRespond("it_works");
    }

    @GET
    @Path("get_length/{message}")
    public Response getLength(@PathParam("message") String message) {
        return stringRespond(message.length());
    }

    @GET
    @Path("print_message/{message}")
    @Produces("application/json")
    public Response print(@PathParam("message") String message) {
        return respond(message);
    }

    @POST
    @Path("post/")
    @Consumes("application/json")
    public Response testPost(String json) {
        return stringRespond("post_works");
    }

    @POST
    @Path("post_json/")
    @Consumes("application/json")
    @Produces("application/json")
    public Response testPostJson(String json) {
        // received message is already in JSON format
        return stringRespond(json);
    }

    @GET
    @Path("produce_format/{message}")
    @Produces("application/json")
    public Response testProduces(@PathParam("message") String message) {
        System.out.println("received:" + message);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("depth_0", "zero");
        jsonObject.put("length", message.length());

        JSONObject nestedJsonObject = new JSONObject();
        nestedJsonObject.put("depth_1", "one");
        nestedJsonObject.put("message", message);
        jsonObject.put("child_json", nestedJsonObject);
        return Response.ok(jsonObject.toString(), MediaType.APPLICATION_JSON).build();
    }

    private Response respond(Object message) {
        Gson gson = new Gson();
        String json = gson.toJson(message);
        return Response.ok(json).build();
    }

    private Response stringRespond(Object message) {
        return Response.ok(message).build();
    }

    public static void main(String[] args) throws Exception {
        int port = 1992;
        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        }
        testREST ra = new testREST(1992);
        ra.start();
    }
}
