package tutorial20;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.security.CertificateSecurityContext;

// Test to copy a file from and to the SE storage.
// You probably need to change the lfnroot and context to run this,
// as these are now set for me.
//
public class LfnFileCopy {
    // Ask the user for the password needed to perform grid-proxy-init
    private static String getPassphrase() {
        JPasswordField pwd = new JPasswordField();

        Object[] message = { "grid-proxy-init\nPlease enter your passphrase.",
                pwd };

        JOptionPane.showMessageDialog(null, message, "Grid-Proxy-Init",
                JOptionPane.QUESTION_MESSAGE);
        return new String(pwd.getPassword());
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
        String lfnroot = "lfn:/grid/pvier/ceriel/";

        // Create a new CertificateSecurityContext containing the globus certificates
        // and the user password.
        CertificateSecurityContext securityContext = new CertificateSecurityContext(
                new URI(System.getProperty("user.home") + "/.globus/userkey.pem"),
                new URI(System.getProperty("user.home") + "/.globus/usercert.pem"),
                getPassphrase());

        // Store this SecurityContext in a GATContext
        GATContext context = new GATContext();
        context.addSecurityContext(securityContext);

        context.addPreference("VirtualOrganisation", "pvier");
        context.addPreference("vomsServerUrl", "voms.grid.sara.nl");
        context.addPreference("vomsServerPort", "30000");
        context.addPreference("vomsHostDN", "/O=dutchgrid/O=hosts/OU=sara.nl/CN=voms.grid.sara.nl");
        context.addPreference("LfcServer", "lfc.grid.sara.nl");
        context.addPreference("bdiiURI", "ldap://bdii.grid.sara.nl:2170");
        // context.addPreference("ftp.connection.passive", "false");
        // context.addPreference("ftp.server.noauthentication", "true");
        // context.addPreference("gridftp.authenticate.retry", "5");

//        context.addPreference("preferredSEID", "srm.grid.sara.nl");

        GAT.setDefaultGATContext(context);

        File exampleFile = GAT.createFile(lfnroot + "text.txt");
        exampleFile.copy(new URI("localtext.txt"));
        File exampleFile2 = GAT.createFile("localtext.txt");
        exampleFile2.copy(new URI(lfnroot + "data"));

        GAT.end();
	}
}
