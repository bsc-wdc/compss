package tutorial20;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.FileInputStream;

class RemoteCat {
    public static void main(String[] args) throws Exception {
        FileInputStream in = GAT.createFileInputStream(new URI(args[0]));
        java.io.InputStreamReader reader = new java.io.InputStreamReader(in);
        java.io.BufferedReader buf = new java.io.BufferedReader(reader);
        while (true) {
            String result = buf.readLine();
            if (result == null) {
                break;
            }
            System.out.println(result);
        }
        in.close();
        GAT.end();
    }
}
