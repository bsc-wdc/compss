package guidance.executors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;

import guidance.GuidanceImpl;


public class CreateListOfExcludedSnpsExecutor extends Executor {

    public CreateListOfExcludedSnpsExecutor(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void execute(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        String exclCgatFlag = (String) extraParams[0];
        String exclSVFlag = (String) extraParams[1];
        // final int rsIdIndex = (int) extraParams[2];
        final int posIndex = (int) extraParams[3];
        final int a1Index = (int) extraParams[4];
        final int a2Index = (int) extraParams[5];
        final String separator = (String) extraParams[6];

        // Array of string to store positions of SNPs to exclude
        ArrayList<String> excludeList = new ArrayList<>();

        String line = null;
        while ((line = br.readLine()) != null) {
            String[] splitted = line.split(separator);
            // String allele1 = splitted[a1Index];
            // String allele2 = splitted[a2Index];
            String positionS = splitted[posIndex];
            boolean excludeSNP = false;

            if (exclSVFlag.equals(GuidanceImpl.FLAG_YES)) {
                if (!splitted[a1Index].equals("A") && !splitted[a1Index].equals("C") && !splitted[a1Index].equals("G")
                        && !splitted[a1Index].equals("T")) {
                    // This SNP is a SV because:
                    // 1) It has more than one point: e.g "AAA.." or "ACG..."
                    // 2) it is a deletion: e.g "-"
                    excludeSNP = true;
                }
                if (!splitted[a2Index].equals("A") && !splitted[a2Index].equals("C") && !splitted[a2Index].equals("G")
                        && !splitted[a2Index].equals("T")) {
                    // This SNP is a SV because:
                    // 1) It has more than one point: e.g "AAA.." or "ACG..."
                    // 2) it is a deletion: e.g "-"
                    excludeSNP = true;
                }
            }

            String allele = splitted[a1Index] + splitted[a2Index];
            if (exclCgatFlag.equals(GuidanceImpl.FLAG_YES)) {
                // Then we have to see if allele is AT TA GC CG to exclude it.
                if (allele.equals("AT") || allele.equals("TA") || allele.equals("GC") || allele.equals("CG")) {
                    excludeSNP = true;
                }
            }

            if (excludeSNP) {
                // Store the positon in the excludeList
                excludeList.add(positionS);
            }
        } // End while readline

        for (int i = 0; i < excludeList.size(); i++) {
            bw.write(excludeList.get(i) + "\n");
        }
        bw.flush();
    }
}
