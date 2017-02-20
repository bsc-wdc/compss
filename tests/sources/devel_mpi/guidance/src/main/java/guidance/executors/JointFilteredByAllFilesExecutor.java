package guidance.executors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;


public class JointFilteredByAllFilesExecutor extends Executor {

    public JointFilteredByAllFilesExecutor(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void execute(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        String rPanelName = (String) extraParams[0];
        boolean putRefpanel = false;

        // I read the header
        String line = br.readLine();
        // I put the refpanel column in the header:
        String[] splittedHeader = line.split("\t");
        if (!splittedHeader[splittedHeader.length - 1].equals("refpanel")) {
            line = line + "\trefpanel";
            putRefpanel = true;
        }

        // Put the header in the output file.
        bw.write(line);
        bw.newLine();

        while ((line = br.readLine()) != null) {
            if (putRefpanel == true) {
                line = line + "\t" + rPanelName;
            }
            bw.write(line);
            bw.newLine();
        }

        bw.flush();
    }
}
