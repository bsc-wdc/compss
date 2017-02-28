package nmmb;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Binary;
import nmmb.configuration.NMMBEnvironment;
import nmmb.fixed.utils.BinaryWrapper;
import nmmb.fixed.utils.FortranWrapper;


public interface NmmbItf {
    
    @Binary(binary = FortranWrapper.FC)
    Integer fortranCompiler(
            @Parameter(type = Type.STRING, direction = Direction.IN) String mcFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String sharedFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String covertPrefix, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String convertValue,
            @Parameter(type = Type.STRING, direction = Direction.IN) String tracebackFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String assumePrefix, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String assumeValue, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String optFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String fpmodelPrefix, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String fpmodelValue,
            @Parameter(type = Type.STRING, direction = Direction.IN) String stackFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String oFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String executable, 
            @Parameter(type = Type.FILE, direction = Direction.IN) String source
        );   

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.SMMOUNT + FortranWrapper.SUFFIX_EXE)
    Integer smmount(
        @Parameter(type = Type.STRING, direction = Direction.IN) String topoDir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String seamaskDEM,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String heightDEM
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.LANDUSE + FortranWrapper.SUFFIX_EXE)
    Integer landuse(
        @Parameter(type = Type.STRING, direction = Direction.IN) String landuseDir,  
        @Parameter(type = Type.FILE, direction = Direction.OUT) String landuse,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String kount_landuse
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.LANDUSENEW + FortranWrapper.SUFFIX_EXE)
    Integer landusenew(
        @Parameter(type = Type.STRING, direction = Direction.IN) String gtopDir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String landusenew,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String kount_landusenew
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.TOPO + FortranWrapper.SUFFIX_EXE)
    Integer topo(
        @Parameter(type = Type.STRING, direction = Direction.IN) String topoDir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String heightmean
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.STDH + FortranWrapper.SUFFIX_EXE)
    Integer stdh(
        @Parameter(type = Type.FILE, direction = Direction.IN) String seamaskDEM,
        @Parameter(type = Type.FILE, direction = Direction.IN) String heightmean,
        @Parameter(type = Type.STRING, direction = Direction.IN) String topoDir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String stdh
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.ENVELOPE + FortranWrapper.SUFFIX_EXE)
    Integer envelope(
        @Parameter(type = Type.FILE, direction = Direction.IN) String heightmean,
        @Parameter(type = Type.FILE, direction = Direction.IN) String stdh,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String height
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.TOPSOILTYPE + FortranWrapper.SUFFIX_EXE)
    Integer topsoiltype(
        @Parameter(type = Type.FILE, direction = Direction.IN) String seamaskDEM,
        @Parameter(type = Type.STRING, direction = Direction.IN) String soiltypeDir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String topsoiltype
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.BOTSOILTYPE + FortranWrapper.SUFFIX_EXE)
    Integer botsoiltype(
        @Parameter(type = Type.FILE, direction = Direction.IN) String seamaskDEM,
        @Parameter(type = Type.STRING, direction = Direction.IN) String soiltypePath,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String botsoiltype
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.TOPOSEAMASK + FortranWrapper.SUFFIX_EXE)
    Integer toposeamask(
        @Parameter(type = Type.FILE, direction = Direction.IN) String seamaskDEM,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String seamask,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String height,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String landuse,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String topsoiltype,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String botsoiltype
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.STDHTOPO + FortranWrapper.SUFFIX_EXE)
    Integer stdhtopo(
        @Parameter(type = Type.FILE, direction = Direction.IN) String seamask,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String stdh
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.DEEPTEMPERATURE + FortranWrapper.SUFFIX_EXE)
    Integer deeptemperature(
        @Parameter(type = Type.FILE, direction = Direction.IN) String seamask,
        @Parameter(type = Type.STRING, direction = Direction.IN) String soiltempPath, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String deeptemperature
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.SNOWALBEDO + FortranWrapper.SUFFIX_EXE)
    Integer snowalbedo(
        @Parameter(type = Type.STRING, direction = Direction.IN) String maxsnowalbDir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String snowalbedo
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.VCGENERATOR + FortranWrapper.SUFFIX_EXE)
    Integer vcgenerator(
        @Parameter(type = Type.FILE, direction = Direction.OUT) String dsg
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.ROUGHNESS + FortranWrapper.SUFFIX_EXE)
    Integer roughness(
        @Parameter(type = Type.STRING, direction = Direction.IN) String roughnessDir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String roughness
    );

    @Binary(binary = NMMBEnvironment.FIX_FOR_ITF + FortranWrapper.GFDLCO2 + FortranWrapper.SUFFIX_EXE)
    Integer gfdlco2(
        @Parameter(type = Type.FILE, direction = Direction.IN) String dsg,
        @Parameter(type = Type.STRING, direction = Direction.IN) String co2_data_dir,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String co2_trans
    );

    @Binary(binary = NMMBEnvironment.LOOKUP_TABLES_DIR_FOR_ITF + BinaryWrapper.RUN_AEROSOL)
    Integer run_aerosol(
        @Parameter(type = Type.FILE, direction = Direction.OUT) String lookup_aerosol2_rh00,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String lookup_aerosol2_rh50, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String lookup_aerosol2_rh70, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String lookup_aerosol2_rh80,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String lookup_aerosol2_rh90, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String lookup_aerosol2_rh95, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String lookup_aerosol2_rh99
    );
    
}
