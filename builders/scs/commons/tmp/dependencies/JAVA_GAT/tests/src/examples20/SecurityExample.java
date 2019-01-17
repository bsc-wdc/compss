package examples20;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.security.CertificateSecurityContext;
import org.gridlab.gat.security.PasswordSecurityContext;

public class SecurityExample {

    /**
     * This example shows the use of the SecurityContext objects in JavaGAT.
     * 
     * This example will attach various security contexts to a GATContext. Then
     * it will try to create the same file with and without this GATContext.
     * Note that you've to replace the arguments of the methods with values that
     * are suitable for your environment. Please don't put passwords and
     * passphrases directly in the source code, but try to retrieve by having
     * the user to type it.
     * 
     * @param args
     *                a String representation of a valid JavaGAT uri that points
     *                to a file
     * 
     */
    public static void main(String[] args) {
        GATContext context = new GATContext();
        try {
            // create a certificate security context
            CertificateSecurityContext globusSecurityContext = new CertificateSecurityContext(
                    new URI("userkey.pem"), new URI("usercert.pem"),
                    "grid-proxy-init passphrase");
            // add a note to this security context; it's only to be used for the
            // globus and wsgt4new adaptors
            globusSecurityContext.addNote("adaptors", "globus,wsgt4new");
            context.addSecurityContext(globusSecurityContext);
        } catch (URISyntaxException e) {
            // ignore
        }
        // create a password security context
        PasswordSecurityContext ftpSecurityContext = new PasswordSecurityContext(
                System.getProperty("user.name"), "ftp password");
        // add a note to this security context; it's only to be used for ftp and
        // only for two hosts, host1 and host2:21
        ftpSecurityContext.addNote("adaptors", "ftp");
        ftpSecurityContext.addNote("hosts", "host1,host2:21");
        context.addSecurityContext(ftpSecurityContext);

        PasswordSecurityContext sshSecurityContext = new PasswordSecurityContext(
                System.getProperty("user.name"), "ssh password");
        // add a note to this security context; it's only to be used for
        // sshtrilead and commandlinessh and only for one host, host3
        sshSecurityContext.addNote("adaptors", "sshtrilead,commandlinessh");
        sshSecurityContext.addNote("hosts", "host3");
        context.addSecurityContext(sshSecurityContext);

        // now create a file using the context which has all the security
        // contexts attached
        try {
            File file = GAT.createFile(context, new URI(args[0]));
            System.out.println(args[0] + " exists: " + file.exists());
        } catch (Exception e) {
            System.err.println("Failed to check whether '" + args[0]
                    + "' exists: " + e);
        }

        // and try to create the same file without the context which has all the
        // security contexts, JavaGAT will try to install some default security
        // contexts.
        try {
            File file2 = GAT.createFile(new URI(args[0]));
            System.out.println(args[0] + " exists: " + file2.exists());
        } catch (Exception e) {
            System.err.println("Failed to check whether '" + args[0]
                    + "' exists: " + e);
        }

    }
}
