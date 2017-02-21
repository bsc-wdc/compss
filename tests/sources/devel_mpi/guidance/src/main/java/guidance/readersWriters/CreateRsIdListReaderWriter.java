package guidance.readersWriters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import guidance.GuidanceImpl;


public class CreateRsIdListReaderWriter extends ReaderWriter {

    public CreateRsIdListReaderWriter(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void readWrite(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        String exclCgatFlag = (String) extraParams[0];
        String inputFormat = (String) extraParams[1];

        if (inputFormat.equals("BED")) {
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] splittedLine = line.split("\t"); // delimiter I assume single space.
                String allele = splittedLine[4] + splittedLine[5]; // store Allele (AC,GC,AG, GT,..., etc.)

                // Store rsID of the SNP which its allele is AT or TA or GC or CG into the .pairs file
                if (exclCgatFlag.equals(GuidanceImpl.FLAG_YES)) {
                    // Then we have to see if allele is AT TA GC CG to put the rsID into the .pairs file.
                    if (allele.equals("AT") || allele.equals("TA") || allele.equals("GC") || allele.equals("CG")) {
                        bw.write(splittedLine[1]);
                        bw.newLine();
                    }
                }
            }
        } else if (inputFormat.equals("GEN")) {
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] splittedLine = line.split(" ");// delimiter I assume single space.
                String allele = splittedLine[3] + splittedLine[4]; // store Allele (AC,GC,AG, GT,..., etc.)

                // Store rsID of the SNP which its allele is AT or TA or GC or CG into the .pairs file
                if (exclCgatFlag.equals(GuidanceImpl.FLAG_YES)) {
                    // Then we have to see if allele is AT TA GC CG to put the rsID into the .pairs file.
                    if (allele.equals("AT") || allele.equals("TA") || allele.equals("GC") || allele.equals("CG")) {
                        bw.write(splittedLine[1]);
                        bw.newLine();
                    }
                }
            }
        } else {
            throw new IOException("[createRsIdList] Error, It was not possible to generate pairsFile. The input is not valid!!");
        }

        bw.flush();
    }
}
