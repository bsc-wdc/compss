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
package es.bsc.compss.agent.rest;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.agent.Agent;
import es.bsc.compss.agent.rest.messages.StartApplicationRequest;
import es.bsc.compss.types.ApplicationParameter;
import es.bsc.compss.types.Resource;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


@Path("/COMPSs")
public class RESTAgent {

    public static final String COMPSS_AGENT_PORT = "COMPSS_AGENT_PORT";

//    private static final ClientConfig config = new ClientConfig();
//    private static final Client client = ClientBuilder.newClient(config);
    @GET
    @Path("test/")
    public Response test() {
        return Response.ok().build();
    }

    @PUT
    @Path("startApplication/")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startApplication(StartApplicationRequest request) {
        Response response;
        String ceiClass = request.getCeiClass();
        if (ceiClass != null) {
            response = runMain(request);
        } else {
            response = runTask(request);
        }
        return response;
    }

    private static Response runMain(StartApplicationRequest request) {
        String serviceInstanceId = request.getServiceInstanceId();
        String ceiClass = request.getCeiClass();

        String className = request.getClassName();
        String methodName = request.getMethodName();
        Object[] params;
        try {
            params = request.getParamsValuesContent();
        } catch (Exception cnfe) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
                    "Could not recover an input parameter value. " + cnfe.getLocalizedMessage()
            ).build();
        }
        Resource[] resources = request.getResources();
        AppMainMonitor monitor = new AppMainMonitor(serviceInstanceId, methodName);
        long appId;
        try {
            appId = Agent.runMain(Lang.JAVA, ceiClass, className, methodName, params, resources, monitor);
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        return Response.ok(appId, MediaType.TEXT_PLAIN).build();
    }

    private static Response runTask(StartApplicationRequest request) {
        String className = request.getClassName();
        String methodName = request.getMethodName();
        ApplicationParameter[] sarParams = request.getParams();
        ApplicationParameter target = request.getTarget();
        boolean hasResult = request.isHasResult();
        Resource[] resources = request.getResources();
        long appId;
        AppTaskMonitor monitor = new AppTaskMonitor(sarParams.length);
        try {
            appId = Agent.runTask(Lang.JAVA, className, methodName, sarParams, target, hasResult, resources, monitor);
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        return Response.ok(appId, MediaType.TEXT_PLAIN).build();
    }

    @PUT
    @Path("endApplication/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response endApplication() {
        return Response.ok().build();
    }

    public static void main(String[] args) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        int port = Integer.parseInt(args[0]);
        System.setProperty("COMPSS_AGENT_PORT", args[0]);
        Server jettyServer = new Server(port);
        jettyServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter("jersey.config.server.provider.classnames",
                RESTAgent.class.getCanonicalName());

        try {
            jettyServer.start();
            jettyServer.join();
        } finally {
            jettyServer.destroy();
        }
    }
}
