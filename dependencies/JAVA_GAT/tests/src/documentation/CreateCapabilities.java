package documentation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.gridlab.gat.AdaptorInfo;
import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;

public class CreateCapabilities {

    /**
     * @param args
     * @throws IOException
     * @throws GATInvocationException
     */
    public static void main(String[] args) throws IOException,
            GATInvocationException {
        File htmlFile = new File("doc" + File.separator
                + "adaptor-capabilities.html");
        if (!htmlFile.exists()) {
            htmlFile.createNewFile();
        }
        FileOutputStream out = new FileOutputStream(htmlFile);
        out.write("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01//EN\">\n"
                .getBytes());
        out.write("<html>\n".getBytes());
        out.write("<head>\n".getBytes());
        out.write(("<title>JavaGAT capabilities: overview</title>\n")
                .getBytes());
        out.write("</head>\n".getBytes());
        out.write("<body>\n".getBytes());
        out
                .write("<table border=1px cellpadding=5 cellspacing=0>\n"
                        .getBytes());
        out.write("<tr>\n".getBytes());
        out.write("<th>name</th>\n".getBytes());
        out.write("<th>adaptors</th>\n".getBytes());
        out.write("</tr>\n".getBytes());

        for (String gatObjectType : GAT.getAdaptorTypes()) {
            out.write("<tr>\n".getBytes());
            out
                    .write(("<td>"
                            + ("<a href=" + gatObjectType
                                    + "-capabilities.html" + ">"
                                    + gatObjectType + "</a>\n") + "</td>\n")
                            .getBytes());
            out.write("<td>\n".getBytes());
            AdaptorInfo[] adaptorInfos = GAT.getAdaptorInfos(gatObjectType);
            for (AdaptorInfo adaptorInfo : adaptorInfos) {
                out.write((adaptorInfo.getShortName() + "<br />\n").getBytes());
            }
            out.write("</td>\n".getBytes());
            out.write("</tr>\n".getBytes());
        }
        out.write("</table>\n".getBytes());

        for (String gatObjectType : GAT.getAdaptorTypes()) {
            htmlFile = new File("doc" + File.separator + gatObjectType
                    + "-capabilities.html");
            if (!htmlFile.exists()) {
                htmlFile.createNewFile();
            }
            out = new FileOutputStream(htmlFile);
            out.write("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01//EN\">\n"
                    .getBytes());
            out.write("<html>\n".getBytes());
            out.write("<head>\n".getBytes());
            out
                    .write(("<title>JavaGAT capabilities: " + gatObjectType + "</title>\n")
                            .getBytes());
            out.write("</head>\n".getBytes());
            out.write("<body>\n".getBytes());
            out.write(("<h1>" + gatObjectType + " adaptors</h1>\n").getBytes());
            out
                    .write(("<p>This page shows the capabilities of the adaptors that implement the JavaGAT "
                            + gatObjectType + " object. The table below shows the status of the adaptor, which can be 'done', 'w.i.p' (work in progress) or 'untested' (coding is done, needs testing). Furthermore you can see the implementation level (the percentage of implemented methods), and you can see the details of which methods are actually implemented. There is also a <a href=#overview>per method overview</a> (which adaptors implement a certain method)</p>\n")
                            .getBytes());
            // this writes the first table
            out.write("<table border=1px cellpadding=5 cellspacing=0>\n"
                    .getBytes());
            out.write("<tr>\n".getBytes());
            out.write("<th>name</th>\n".getBytes());
            out.write("<th>status</th>\n".getBytes());
            out.write("<th>implemented</th>\n".getBytes());
            // out.write("<td>tests</td>\n".getBytes());
            out.write("</tr>\n".getBytes());
            AdaptorInfo[] adaptorInfos = GAT.getAdaptorInfos(gatObjectType);
            for (AdaptorInfo adaptorInfo : adaptorInfos) {
                out.write("<tr>\n".getBytes());
                out.write(("<td>" + adaptorInfo.getShortName() + "</td>\n")
                        .getBytes());
                out.write(("<td>done</td>\n").getBytes());
                int i = 0;
                if (adaptorInfo.getSupportedCapabilities() != null) {
                    for (String key : adaptorInfo.getSupportedCapabilities()
                            .keySet()) {
                        if (adaptorInfo.getSupportedCapabilities().get(key)) {
                            i++;
                        }
                    }
                    out
                            .write(("<td>"
                                    + (i * 100)
                                    / adaptorInfo.getSupportedCapabilities()
                                            .size() + " % [<a href=#"
                                    + adaptorInfo.getShortName() + ">details</a>]</td>\n")
                                    .getBytes());
                } else {
                    out.write("<td>- %</td>\n".getBytes());
                }

                // out.write(("<td><a href=#>tests</a></td>\n").getBytes());
                out.write("</tr>\n".getBytes());
            }
            out.write("</table>\n".getBytes());

            for (AdaptorInfo adaptorInfo : adaptorInfos) {
                out
                        .write(("<h2><a name=" + adaptorInfo.getShortName()
                                + ">" + adaptorInfo.getShortName() + " - implementation details</a></h2>\n")
                                .getBytes());
                out.write(("<p>Adaptor description: " + adaptorInfo
                        .getDescription()).getBytes());
                out.write("<table border=1px cellpadding=5 cellspacing=0>\n"
                        .getBytes());
                out.write("<tr>\n".getBytes());
                out.write("<th>implemented</th>\n".getBytes());
                out.write("<th>not implemented</th>\n".getBytes());
                out.write("</tr>\n".getBytes());
                if (adaptorInfo.getSupportedCapabilities() != null) {
                    for (String key : adaptorInfo.getSupportedCapabilities()
                            .keySet()) {
                        out.write("<tr>\n".getBytes());
                        if (!adaptorInfo.getSupportedCapabilities().get(key)) {
                            out.write("<td>-</td>\n".getBytes());
                        }
                        out.write(("<td>" + key + "</td>\n").getBytes());
                        if (adaptorInfo.getSupportedCapabilities().get(key)) {
                            out.write("<td>-</td>\n".getBytes());
                        }
                        out.write("</tr>\n".getBytes());
                    }
                }
                out.write("</table>\n".getBytes());
            }

            out.write("<a name=overview><h2>Per Method Overview</h2></a>\n"
                    .getBytes());
            out.write("<table border=1px cellpadding=5 cellspacing=0>\n"
                    .getBytes());
            out.write("<tr>\n".getBytes());
            out.write("<td></td>\n".getBytes());
            Set<String> methods = new HashSet<String>();

            for (AdaptorInfo adaptorInfo : adaptorInfos) {
                out
                        .write(("<th>"
                                + adaptorInfo.getShortName().substring(0, 5) + "</th>\n")
                                .getBytes());
                if (adaptorInfo.getSupportedCapabilities() != null) {
                    methods.addAll(adaptorInfo.getSupportedCapabilities()
                            .keySet());
                }
            }
            out.write("</tr>\n".getBytes());
            for (String method : methods) {
                out.write("<tr>\n".getBytes());
                out.write(("<td>" + method + "</td>\n").getBytes());
                for (AdaptorInfo adaptorInfo : adaptorInfos) {
                    if (!adaptorInfo.getSupportedCapabilities().containsKey(
                            method)) {
                        out.write(("<td>?</td>\n").getBytes());
                    } else if (adaptorInfo.getSupportedCapabilities().get(
                            method)) {
                        out
                                .write(("<td><font color=#00FF00><b>V</b></font></td>\n")
                                        .getBytes());
                    } else {
                        out
                                .write(("<td><font color=#FF0000><b>X</b></font></td>\n")
                                        .getBytes());
                    }
                }
                out.write("</tr>\n".getBytes());
            }
            out.write("<tr>\n".getBytes());
            out.write("<td>total</td>\n".getBytes());
            for (AdaptorInfo adaptorInfo : adaptorInfos) {
                int i = 0;
                if (adaptorInfo.getSupportedCapabilities() != null) {
                    for (String key : adaptorInfo.getSupportedCapabilities()
                            .keySet()) {
                        if (adaptorInfo.getSupportedCapabilities().get(key)) {
                            i++;
                        }
                    }
                    out
                            .write(("<td>"
                                    + (i * 100)
                                    / adaptorInfo.getSupportedCapabilities()
                                            .size() + " %</td>\n").getBytes());
                } else {
                    out.write("<td>no info</td>\n".getBytes());
                }
            }
            out.write("</tr>\n".getBytes());

            out.write("</table>\n".getBytes());
            out.write("</body>\n".getBytes());
            out.write("</html>\n".getBytes());
            System.out
                    .println("capabilities written to: " + htmlFile.getPath());
        }

    }
}
