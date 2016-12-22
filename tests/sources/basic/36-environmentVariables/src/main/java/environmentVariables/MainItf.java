package environmentVariables;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


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
