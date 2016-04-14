package org.gridlab.gat.resources.cpi.sshlsf;

import java.io.PrintWriter;
import java.io.Writer;

public class LSFScriptWriter extends PrintWriter {
    
    private static final String slurmSuffix = "#BSUB";

    public LSFScriptWriter (Writer out) {
    	super(out);
    }
    
    /**
     * Adds an option line for Slurm.
     * @param opt the option itself.
     * @param param an optional additional parameter.
     */
    public void addOption(String opt, Object param) {
        print(slurmSuffix);
        print(" -");
        print(opt);
        if (param != null) {
            print(" ");
            print(param);
        }
        print('\n');
    }

    /**
     * Adds an option line for Slurm.
     * @param s the full option string.
     */
    public void addString(String s) {
	print(slurmSuffix);
	print (" ");
	print(s);
	print('\n');
    }
}