package examples20;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.advert.AdvertService;
import org.gridlab.gat.io.Endpoint;
import org.gridlab.gat.io.Pipe;

public class EndpointExample {

    /**
     * This example shows the use of Endpoints in JavaGAT. It should be started
     * twice.
     * 
     * Once on the 'local' machine, this instance needs one argument, the
     * hostname of the 'remote' machine. It then creates an Endpoint, stores it
     * in an AdvertService exports the advert database to the remote machine and
     * listens for incoming connections on this Endpoint. Upon a connection
     * establishment, it tries to read a line from the Pipe, which should
     * contain the text 'hello world'.
     * 
     * On the 'remote' machine the second instance of this example should be
     * started, without any arguments. It tries to get the advertised Endpoint
     * out of the imported advert database. It uses this Endpoint to connect
     * (back) to the 'local' machine. It writes the text 'hello world' into the
     * Pipe and then closes it.
     * 
     * @param args
     *                if 'local' instance, the first argument should contain the
     *                hostname of the 'remote' instance.
     * @throws IOException
     * @throws URISyntaxException
     * @throws GATInvocationException
     * @throws GATObjectCreationException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws GATObjectCreationException,
            GATInvocationException, URISyntaxException, IOException,
            InterruptedException {
        boolean local = args.length > 0;
        if (local) {
            new EndpointExample().local(args[0]);
        } else {
            new EndpointExample().remote();
        }
        GAT.end();
    }

    public void local(String remoteHost) throws GATObjectCreationException,
            GATInvocationException, URISyntaxException, IOException {
        System.err.println("Starting the 'local' instance.");
        Endpoint endpoint = GAT.createEndpoint();
        AdvertService advert = GAT.createAdvertService();
        advert.add(endpoint, null, "examples/endpoint");
        advert.exportDataBase(new URI("any://" + remoteHost
                + "/example-advert.db"));
        System.err.println("Advert database exported to: '" + "any://"
                + remoteHost + "/example-advert.db" + "'.");
        System.err
                .println("Setup ready. Listening for incoming connection from 'remote' instance");
        Pipe p = endpoint.listen();
        System.err.println("Connection established. Trying to read a line.");
        BufferedReader in = new BufferedReader(new InputStreamReader(p
                .getInputStream()));
        System.out.println("read: " + in.readLine());
        p.close();
    }

    public void remote() throws GATObjectCreationException,
            GATInvocationException, URISyntaxException, IOException,
            InterruptedException {
        System.err.println("Starting the 'remote' instance.");
        AdvertService advert = GAT.createAdvertService();
        advert.importDataBase(new URI("any://localhost/example-advert.db"));
        System.err
                .println("Advert database imported from 'any://localhost/example-advert.db'.");
        Endpoint endpoint = (Endpoint) advert
                .getAdvertisable("examples/endpoint");
        System.err.println("Endpoint retrieved from the advert service.");
        Pipe p = endpoint.connect();
        System.err.println("Connection established. Writing 'hello world'.");
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(p
                .getOutputStream()));
        out.write("hello world\n");
        out.flush();
        Thread.sleep(10 * 1000);
        p.close();
    }

}
