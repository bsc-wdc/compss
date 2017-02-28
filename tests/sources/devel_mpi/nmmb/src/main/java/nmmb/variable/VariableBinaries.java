package nmmb.variable;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class VariableBinaries {

	public static void degribgfs_generic_05(String cW, String iCEC, String sH,
			String sOILT2, String sOILT4, String sOILW2, String sOILW4,
			String tT, String vV, String hH, String pRMSL, String sOILT1,
			String sOILT3, String sOILW1, String sOILW3, String sST_TS,
			String uU, String wEASD) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/degribgfs_generic_05.sh" + " " 
		        + cW + " " + iCEC + " " + sH + " " + sOILT2 + " " + sOILT4 + " " + sOILW2 + " " + sOILW4 + " " + tT + " " 
				+ vV + " " + hH + " " + pRMSL + " " + sOILT1 + " " + sOILT3 + " " + sOILW1 + " " + sOILW3 + " " + sST_TS + " " 
		        + uU + " " + wEASD;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void gfs2model_rrtm(String cW, String iCEC, String sH,
			String sOILT2, String sOILT4, String sOILW2, String sOILW4,
			String tT, String vV, String hH, String pRMSL, String sOILT1,
			String sOILT3, String sOILW1, String sOILW3, String sST_TS,
			String uU, String wEASD, String gFS_file) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/gfs2model_rrtm.exe" + " " 
		        + cW + " " + iCEC + " " + sH + " " + sOILT2 + " " + sOILT4 + " " + sOILW2 + " " + sOILW4 + " " + tT + " " 
				+ vV + " " + hH + " " + pRMSL + " " + sOILT1 + " " + sOILT3 + " " + sOILW1 + " " + sOILW3 + " " + sST_TS + " " 
		        + uU + " " + wEASD + " " + gFS_file;
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			
			InputStream fis = bin.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line;
			
			while ((line = br.readLine()) != null)   {
				System.out.println(line);
			}
			
			InputStream fis1 = bin.getErrorStream();
			BufferedReader br1 = new BufferedReader(new InputStreamReader(fis1));
			
			while ((line = br1.readLine()) != null)   {
				System.out.println(line);
			}
			
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void inc_rrtm(String gFS_file, String Deco) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/inc_rrtm.x" + " " + gFS_file + " " + Deco ;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void cnv_rrtm(String gFS_file, String llspl000,
			String outtmp, String outmst, String outsst, String outsno,
			String outcic, String deco) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/cnv_rrtm.x" + " " + gFS_file  
				+ " " +  llspl000 + " " +  outtmp + " " +  outmst + " " +  outsst + " " +  outsno + " " +  outcic + " " +  deco;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void degribsst(String llgsst05) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/degribsst.x" + " " + llgsst05;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void albedo(String llspl000, String seamask, String albedo,
			String albedobase) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/albedo.x" + " " + llspl000 
				+ " " + seamask + " " + albedo + " " + albedobase;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void albedorrtm(String llspl000, String seamask,
			String albedorrtm) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/albedorrtm1deg.x" + " " + llspl000 
				+ " " + seamask + " " + albedorrtm;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void vegfrac(String llspl000, String seamask, String vegfrac) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/vegfrac.x" + " " + llspl000 
				+ " " + seamask + " " + vegfrac;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void z0vegfrac(String seamask, String landuse,
			String topsoiltype, String height, String stdh, String vegfrac,
			String z0base, String z0, String ustar) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/z0vegustar.x" + " " + seamask 
				+ " " + landuse + " " + topsoiltype + " " + height 
				+ " " + stdh + " " + vegfrac + " " + z0base + " " + z0 + " " + ustar ;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void allprep(String llspl000, String llgsst05, String sst05,
			String height, String seamask, String stdh,
			String deeptemperature, String snowalbedo, String z0,
			String z0base, String landuse, String landusenew,
			String topsoiltype, String vegfrac, String albedorrtm,
			String llgsst, String lgsno, String llgcic, String llsmst,
			String llstmp, String albedorrtmcorr, String dzsoil, String tskin,
			String sst, String snow, String snowheight, String cice,
			String seamaskcorr, String landusecorr, String landusenewcorr,
			String topsoiltypecorr, String vegfraccorr, String z0corr,
			String z0basecorr, String emissivity, String canopywater,
			String frozenprecratio, String smst, String sh2o, String stmp,
			String dsg, String fcst, String albedo, String ustar) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/allprep_rrtm.x" + " " 
		        + llspl000 + " " + llgsst05 + " " + sst05 + " " + height + " " + seamask + " " + stdh + " " + deeptemperature + " " + snowalbedo + " " 
				+ z0 + " " + z0base + " " + landuse + " " + landusenew + " " + topsoiltype + " " + vegfrac + " " + albedorrtm + " " + llgsst + " " 
		        + lgsno + " " + llgcic + " " + llsmst + " " + llstmp + " " + albedorrtmcorr + " " + dzsoil + " " + tskin + " " + sst + " " + snow + " " 
				+ snowheight + " " + cice + " " + seamaskcorr + " " + landusecorr + " " + landusenewcorr + " " + topsoiltypecorr + " " + vegfraccorr + " " + z0corr + " " 
				+ z0basecorr + " " + emissivity + " " + canopywater + " " + frozenprecratio + " " + smst + " " + sh2o + " " + stmp + " " + dsg + " " + fcst + " " 
				+ albedo + " " + ustar ;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			
			InputStream fis = bin.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line;
			
			while ((line = br.readLine()) != null)   {
				System.out.println(line);
			}
			
			InputStream fis1 = bin.getErrorStream();
			BufferedReader br1 = new BufferedReader(new InputStreamReader(fis1));
			
			while ((line = br1.readLine()) != null)   {
				System.out.println(line);
			}
			
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void readpaulsource(String seamask, String source, String sourceNETCDF) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/read_paul_source.x" + " " + seamask 
				+ " " + source + " " + sourceNETCDF;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void dust_start(String llspl000, String soildust,
			String snow, String topsoiltypecorr, String landusecorr,
			String landusenewcorr, String kount_landuse,
			String kount_landusenew, String vegfrac, String height,
			String seamask, String source, String z0corr, String roughness) {
		
		String cmd = "/gpfs/projects/bsc32/bsc32353/NMMB/fixed_compss/variable808_clean/exe/dust_start.x" + " " + llspl000 
				+ " " + soildust + " " + snow + " " + topsoiltypecorr + " " + landusecorr + " " + landusenewcorr + " " + kount_landuse + " " 
				+ kount_landusenew + " " + vegfrac + " " + height + " " + seamask + " " + source + " " + z0corr + " " + roughness  ;
		
		System.out.println(cmd);
		
		Process bin = null;
		
		try {
			System.out.println("Running command: " + cmd);
			bin = Runtime.getRuntime().exec(cmd);
			int exitValue = bin.waitFor();
			if (exitValue != 0) System.err.println("Exit value is " + exitValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
