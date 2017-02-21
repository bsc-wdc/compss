package guidance.readersWriters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Hashtable;

import guidance.utils.ChromoInfo;
import guidance.utils.HashUtils;


public class CombineCondensedFilesReaderWriter extends ReaderWriter {

    public CombineCondensedFilesReaderWriter(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void readWrite(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        Double mafThreshold = (Double) extraParams[0];
        Double infoThreshold = (Double) extraParams[1];
        Double hweCohortThreshold = (Double) extraParams[2];
        Double hweCasesThreshold = (Double) extraParams[3];
        Double hweControlsThreshold = (Double) extraParams[4];

        // I read the header
        String line = br.readLine();

        Hashtable<String, Integer> inputFileHashTableIndex = HashUtils.createHashWithHeader(line, "\t");
        // Hashtable<Integer, String> inputFileHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(line,
        // "\t");

        String headerCondensed = "CHR\tBP\tP";
        bw.write(headerCondensed);
        bw.newLine();

        while ((line = br.readLine()) != null) {
            String[] splittedLine = line.split("\t");// delimiter I assume single space.
            String chromo = splittedLine[inputFileHashTableIndex.get("chr")];
            String infoS = splittedLine[inputFileHashTableIndex.get("info_all")];

            // We start with these values for hwe values just to allows the X chromosome to pass the if statement of the
            // next lines
            // Just remember that hwe filtering when chromo X is being processed does not make sense.
            String hwe_cohortS = "1.0";
            String hwe_casesS = "1.0";
            String hwe_controlsS = "1.0";

            if (!chromo.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR)) {
                hwe_cohortS = splittedLine[inputFileHashTableIndex.get("cohort_1_hwe")];
                hwe_casesS = splittedLine[inputFileHashTableIndex.get("cases_hwe")];
                hwe_controlsS = splittedLine[inputFileHashTableIndex.get("controls_hwe")];
            }

            String cases_mafS = splittedLine[inputFileHashTableIndex.get("cases_maf")];
            String controls_mafS = splittedLine[inputFileHashTableIndex.get("controls_maf")];

            String position = splittedLine[inputFileHashTableIndex.get("position")];
            String pva = splittedLine[inputFileHashTableIndex.get("frequentist_add_pvalue")];

            String chrbpb = chromo + "\t" + position + "\t" + pva;

            if (!cases_mafS.equals("NA") && !controls_mafS.equals("NA") && !infoS.equals("NA") && !hwe_cohortS.equals("NA")
                    && !hwe_casesS.equals("NA") && !hwe_controlsS.equals("NA") && !pva.equals("NA")) {
                Double cases_maf = Double.parseDouble(cases_mafS);
                Double controls_maf = Double.parseDouble(controls_mafS);
                Double info = Double.parseDouble(infoS);
                Double hweCohort = 1.0;
                Double hweCases = 1.0;
                Double hweControls = 1.0;

                if (!chromo.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR)) {
                    hweCohort = Double.parseDouble(hwe_cohortS);
                    hweCases = Double.parseDouble(hwe_casesS);
                    hweControls = Double.parseDouble(hwe_controlsS);
                }

                // Verificar la condicion
                if (cases_maf >= mafThreshold && controls_maf >= mafThreshold && info >= infoThreshold && hweCohort >= hweCohortThreshold
                        && hweCases >= hweCasesThreshold && hweControls >= hweControlsThreshold) {

                    bw.write(chrbpb);
                    bw.newLine();
                }
            }
        }

        bw.flush();
    }
}
