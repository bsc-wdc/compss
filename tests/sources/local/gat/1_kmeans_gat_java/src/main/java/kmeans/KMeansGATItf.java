package kmeans;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface KMeansGATItf {

    @Method(declaringClass = "kmeans.KMeansGAT")
    void computeNewLocalClusters(@Parameter int myK, @Parameter int numDimensions, @Parameter float[] points,
        @Parameter float[] clusterPoints, @Parameter(direction = Direction.INOUT) float[] newClusterPoints,
        @Parameter(direction = Direction.INOUT) int[] clusterCounts);

    @Method(declaringClass = "kmeans.KMeansGAT")
    void accumulate(@Parameter(direction = Direction.INOUT) float[] onePoints, @Parameter float[] otherPoints,
        @Parameter(direction = Direction.INOUT) int[] oneCounts, @Parameter int[] otherCounts);

    @Method(declaringClass = "kmeans.KMeansGAT")
    float[] initPointsFrag(@Parameter int oneCounts, @Parameter int otherCounts);

}
