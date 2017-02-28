package nmmb.variable;

public class Variable {

	public static void main(String[] args) {
		
		String CW = "../output/00_CW.dump";
		String ICEC = "../output/00_ICEC.dump";
		String SH = "../output/00_SH.dump";
		String SOILT2 = "../output/00_SOILT2.dump";  
		String SOILT4 = "../output/00_SOILT4.dump";  
		String SOILW2 = "../output/00_SOILW2.dump";  
		String SOILW4 = "../output/00_SOILW4.dump";  
		String TT = "../output/00_TT.dump";
		String VV = "../output/00_VV.dump";
		String HH = "../output/00_HH.dump"; 
		String PRMSL = "../output/00_PRMSL.dump";  
		String SOILT1 = "../output/00_SOILT1.dump";  
		String SOILT3 = "../output/00_SOILT3.dump";  
		String SOILW1 = "../output/00_SOILW1.dump";  
		String SOILW3 = "../output/00_SOILW3.dump";  
		String SST_TS = "../output/00_SST_TS.dump";  
		String UU = "../output/00_UU.dump";  
		String WEASD = "../output/00_WEASD.dump";

		VariableBinaries.degribgfs_generic_05(CW,ICEC,SH,SOILT2,SOILT4,SOILW2,SOILW4,TT,VV,HH,PRMSL,SOILT1,SOILT3,SOILW1,SOILW3,SST_TS,UU,WEASD);
		
		String GFS_file = "../output/131140000.gfs";
		
		VariableBinaries.gfs2model_rrtm(CW,ICEC,SH,SOILT2,SOILT4,SOILW2,SOILW4,TT,VV,HH,PRMSL,SOILT1,SOILT3,SOILW1,SOILW3,SST_TS,UU,WEASD,GFS_file);
		
		String deco = "./src/include/deco.inc";
		
		VariableBinaries.inc_rrtm(GFS_file,deco);
		
		String llspl000 = "../output/llspl.000";
		String outtmp="../output/llstmp";
		String outmst="../output/llsmst";
		String outsst="../output/llgsst";
		String outsno="../output/llgsno";
		String outcic="../output/llgcic";

		VariableBinaries.cnv_rrtm(GFS_file,llspl000,outtmp,outmst,outsst,outsno,outcic,deco);
		
		String llgsst05="../output/llgsst05";
		
		VariableBinaries.degribsst(llgsst05);
		
		String seamask="../output/seamask";
		String albedo="../output/albedo";
		String albedobase="../output/albedobase";
		
		VariableBinaries.albedo(llspl000,seamask,albedo,albedobase);
		
		String albedorrtm="../output/albedorrtm";
		
		VariableBinaries.albedorrtm(llspl000,seamask,albedorrtm);
		
		String vegfrac="../output/vegfrac";
		
		VariableBinaries.vegfrac(llspl000,seamask,vegfrac);
		
		String landuse = "../output/landuse";
		String topsoiltype = "../output/topsoiltype";
		String height = "../output/height";	
		String stdh = "../output/stdh";
		String z0base = "../output/z0base";
		String z0 = "../output/z0";
		String ustar = "../output/ustar";
		
		VariableBinaries.z0vegfrac(seamask,landuse,topsoiltype,height,stdh,vegfrac,z0base,z0,ustar);
		
		String sst05 = "../output/sst05";
		String deeptemperature = "../output/deeptemperature";
		String snowalbedo = "../output/snowalbedo";		
		String landusenew = "../output/landusenew";
		String llgsst = "../output/llgsst";
		String llgsno = "../output/llgsno";
		String llgcic = "../output/llgcic";
		String llsmst = "../output/llsmst";
		String llstmp = "../output/llstmp";
		String albedorrtmcorr = "../output/albedorrtmcorr";
		String dzsoil = "../output/dzsoil";
		String tskin = "../output/tskin";
		String sst = "../output/sst";
		String snow = "../output/snow";
		String snowheight= "../output/snowheight";
		String cice = "../output/cice";
		String seamaskcorr = "../output/seamaskcorr";
		String landusecorr = "../output/landusecorr";		
		String landusenewcorr = "../output/landusenewcorr";		
		String topsoiltypecorr = "../output/topsoiltypecorr";
		String vegfraccorr = "../output/vegfraccorr";
		String z0corr = "../output/z0corr";
		String z0basecorr = "../output/z0basecorr";
		String emissivity = "../output/emissivity";
		String canopywater = "../output/canopywater";
		String frozenprecratio = "../output/frozenprecratio";
		String smst = "../output/smst";
		String sh2o = "../output/sh2o";
		String stmp = "../output/stmp";
		String dsg = "../output/dsg";
		String fcst = "../output/fcst";
				
		VariableBinaries.allprep(llspl000,llgsst05,sst05,height,seamask,stdh,deeptemperature,snowalbedo,z0,z0base,landuse,landusenew,
				topsoiltype,vegfrac,albedorrtm,llgsst,llgsno,llgcic,llsmst,llstmp,albedorrtmcorr,dzsoil,tskin,sst,snow,snowheight,cice,
				seamaskcorr,landusecorr,landusenewcorr,topsoiltypecorr,vegfraccorr,z0corr,z0basecorr,emissivity,canopywater,frozenprecratio,
				smst,sh2o,stmp,dsg,fcst,albedo,ustar);
		
		String source = "../output/source";
		String sourceNETCDF = "../output/source.nc";
		
		VariableBinaries.readpaulsource(seamask,source,sourceNETCDF);
		
		String soildust = "../output/soildust";
		String kount_landuse = "../output/kount_landuse";		
		String kount_landusenew = "../output/kount_landusenew";	
		String roughness = "../output/roughness";
		
		VariableBinaries.dust_start(llspl000,soildust,snow,topsoiltypecorr,landusecorr,landusenewcorr,kount_landuse,kount_landusenew,
				vegfrac,height,seamask,source,z0corr,roughness);
		
	}
	
}
