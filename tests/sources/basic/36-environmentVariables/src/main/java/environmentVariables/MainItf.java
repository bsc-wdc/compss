package environmentVariables;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {

    @Method(declaringClass = "environmentVariables.MainImpl")
    @Constraints(computingUnits = "${computingUnits}",
                    processorName = "${processorName}",
                    processorSpeed = "${processorSpeed}",
                    processorArchitecture = "${processorArchitecture}",
                    processorPropertyName = "${processorPropertyName}", 
                    processorPropertyValue = "${processorPropertyValue}",
                    memorySize = "${memorySize}",
                    memoryType = "${memoryType}",
                    storageSize = "${storageSize}",
                    storageType = "${storageType}",
                    operatingSystemType = "${operatingSystemType}",
                    operatingSystemDistribution = "${operatingSystemDistribution}",
                    operatingSystemVersion = "${operatingSystemVersion}",
                    appSoftware = "${appSoftware}",
                    hostQueues = "${hostQueues}",
                    wallClockLimit = "${wallClockLimit}")
    int task(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );
}
