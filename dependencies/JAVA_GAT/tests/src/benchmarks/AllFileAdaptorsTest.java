package benchmarks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import org.gridlab.gat.AdaptorNotApplicableException;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;

public class AllFileAdaptorsTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("usage: <host> <adaptor>,[<adaptor>]");
            System.exit(1);
        }
        String host = args[0];
        String[] adaptors = args[1].split(",");
        AdaptorTestResult[] results = new AdaptorTestResult[adaptors.length];
        int i = 0;
        for (String adaptor : adaptors) {
            if (adaptor.equals("local")) {
                results[i++] = new FileAdaptorTest().test(adaptor, "localhost");
            } else {
                results[i++] = new FileAdaptorTest().test(adaptor, host);
            }
        }
        try {
            printResults(results, host);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printResults(AdaptorTestResult[] results, String host)
            throws IOException {
        File outFile = new File("File-results-" + host + ".html");
        if (!outFile.exists()) {
            outFile.createNewFile();
        }
        FileOutputStream out = new FileOutputStream(outFile);
        out.write("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01//EN\">\n"
                .getBytes());
        out.write("<html>\n".getBytes());
        out.write("<head>\n".getBytes());
        out.write("<title>JavaGAT test results: File</title>\n".getBytes());
        out.write(("<script>\n" + "function showhide(id){\n"
                + "\tif (document.getElementById){\n"
                + "\t\tobj = document.getElementById(id);\n"
                + "\t\tif (obj.style.display == \"none\"){\n"
                + "\t\t\tobj.style.display = \"\";\n" + "\t\t} else {\n"
                + "\t\t\tobj.style.display = \"none\";\n" + "\t\t}\n" + "\t}\n"
                + "}\n" + "</script>\n").getBytes());

        out.write("</head>\n".getBytes());
        out.write("<body>\n".getBytes());
        out.write("<table frame=box cellpadding=5 cellspacing=0>\n".getBytes());
        out.write("<tr>\n".getBytes());
        out.write("<td></td>\n".getBytes());
        for (AdaptorTestResult result : results) {
            out.write(("<td align=right>" + result.getAdaptor() + "</td>\n")
                    .getBytes());
        }
        out.write("</tr>\n".getBytes());
        boolean background = true;
        String[] keys = results[0].getTestResultEntries().keySet().toArray(
                new String[results[0].getTestResultEntries().size()]);
        Arrays.sort(keys);
        for (String key : keys) {
            if (background) {
                out.write("<tr bgcolor=#DDDDDD>\n".getBytes());
            } else {
                out.write("<tr bgcolor=#FFFFFF>\n".getBytes());
            }
            background = !background;
            out.write(("<td>" + key + "</td>\n").getBytes());
            for (AdaptorTestResult result : results) {
                AdaptorTestResultEntry entry = result.getTestResultEntries()
                        .get(key);
                if (entry == null) {
                    out.write("<td align=right>not present</td>\n".getBytes());
                    continue;
                }
                if (entry.getException() == null && !entry.getResult()) {
                    out.write("<td align=right bgcolor=#FFDDDD>".getBytes());
                } else {
                    out.write("<td align=right>".getBytes());
                }
                if (entry.getException() == null) {
                    out.write((entry.getTime() + " ms").getBytes());
                } else {
                    if (entry.getException() instanceof GATInvocationException
                            && ((GATInvocationException) entry.getException())
                                    .getExceptions().length > 0
                            && ((GATInvocationException) entry.getException())
                                    .getExceptions()[0] instanceof UnsupportedOperationException) {
                        out.write("n.i.".getBytes());
                    } else if (entry.getException() instanceof GATObjectCreationException
                            && ((GATObjectCreationException) entry.getException())
                                    .getExceptions().length > 0
                            && ((GATObjectCreationException) entry
                                    .getException()).getExceptions()[0] instanceof GATObjectCreationException
                            && ((GATObjectCreationException) ((GATObjectCreationException) entry
                                    .getException()).getExceptions()[0])
                                    .getExceptions().length > 0
                            && ((GATObjectCreationException) ((GATObjectCreationException) entry
                                    .getException()).getExceptions()[0])
                                    .getExceptions()[0] instanceof AdaptorNotApplicableException) {
                        out.write("n.a.".getBytes());
                    } else {
                        out.write(("<div style=\"display: none;\" id=\""
                                + result.getAdaptor() + key + "\">\n")
                                .getBytes());
                        out.write((entry.getException().toString().replace(
                                "\n", "<br/>\n") + "\n").getBytes());
                        StringWriter s = new StringWriter();
                        PrintWriter p = new PrintWriter(s);
                        entry.getException().printStackTrace(p);
                        out
                                .write((s.toString().replace("\n", "<br/>\n") + "\n")
                                        .getBytes());
                        out.write("</div>\n".getBytes());
                        out
                                .write(("<a href=\"#\" onclick=\"showhide('"
                                        + result.getAdaptor() + key + "'); return(false);\">show/hide</a>\n")
                                        .getBytes());
                    }
                }
                out.write("</td>\n".getBytes());
            }
            out.write("</tr>\n".getBytes());
        }
        out.write("<tr>\n".getBytes());
        out.write("<td>total</td>\n".getBytes());
        for (AdaptorTestResult result : results) {
            out
                    .write(("<td align=right>" + result.getTotalRunTime() + " ms</td>\n")
                            .getBytes());
        }
        out.write("</tr>\n".getBytes());
        out.write("<tr>\n".getBytes());
        out.write("<td>average</td>\n".getBytes());
        for (AdaptorTestResult result : results) {
            out
                    .write(("<td align=right>" + result.getAverageRunTime() + " ms</td>\n")
                            .getBytes());
        }
        out.write("</tr>\n".getBytes());
        out.write("</table>\n".getBytes());
        out.write("</body>\n".getBytes());
        out.write("</html>\n".getBytes());
    }
}
