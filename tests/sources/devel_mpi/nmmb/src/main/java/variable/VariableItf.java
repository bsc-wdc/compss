package variable;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;

public interface VariableItf {
	
	@Method(declaringClass= "variable.VariableBinaries")
	void degribgfs_generic_05(
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String CW,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String ICEC,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SH,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILT2,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILT4,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILW2,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILW4,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String TT,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String VV,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String HH,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String PRMSL,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILT1,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILT3,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILW1,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SOILW3,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String SST_TS,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String UU,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String WEASD
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void gfs2model_rrtm(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String CW,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String ICEC,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SH,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILT2,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILT4,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILW2,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILW4,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String TT,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String VV,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String HH,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String PRMSL,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILT1,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILT3,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILW1,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SOILW3,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String SST_TS,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String UU,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String WEASD,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String GFS_file
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void inc_rrtm(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String GFS_file,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String Deco
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void cnv_rrtm(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String GFS_file,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String llspl000,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String outtmp,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String outmst,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String outsst,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String outsno,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String outcic,		
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String Deco
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void degribsst(
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String llgsst05
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void albedo(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llspl000,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String seamask,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String albedo,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String albedobase
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void albedorrtm(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llspl000,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String seamask,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String albedorrtm
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void vegfrac(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llspl000,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String seamask,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String vegfrac
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void z0vegfrac(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String seamask,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String landuse,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String topsoiltype,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String height,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String stdh,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String vegfrac,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String z0base,		
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String z0,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String ustar
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void allprep(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llspl000,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llgsst05,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String sst05,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String height,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String seamask,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String stdh,
			@Parameter(type = Type.FILE, direction = Direction.INOUT)
			String deeptemperature,
			@Parameter(type = Type.FILE, direction = Direction.INOUT)
			String snowalbedo,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String z0,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String z0base,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String landuse,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String landusenew,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String topsoiltype,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String vegfrac,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String albedorrtm,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llgsst,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llgsno,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llgcic,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llsmst,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llstmp,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String albedorrtmcorr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String dzsoil,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String tskin,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String sst,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String snow,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String snowheight,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String cice,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String seamaskcorr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String landusecorr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String landusenewcorr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String topsoiltypecorr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String vegfraccorr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String z0corr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String z0basecorr,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String emissivity,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String canopywater,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String frozenprecratio,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String smst,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String sh2o,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String stmp,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String dsg,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String fcst,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String albedo,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String ustar
	);
	
	
	@Method(declaringClass= "variable.VariableBinaries")
	void readpaulsource(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String seamask,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String source,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String sourceNETCDF
	);
	
	@Method(declaringClass= "variable.VariableBinaries")
	void dust_start(
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String llspl000,
			@Parameter(type = Type.FILE, direction = Direction.OUT)
			String soildust,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String snow,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String topsoiltypecorr,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String landusecorr,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String landusenewcorr,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String kount_landuse,		
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String kount_landusenew,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String vegfrac,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String height,		
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String seamask,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String source,			
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String z0corr,
			@Parameter(type = Type.FILE, direction = Direction.IN)
			String roughness
	);
	
}