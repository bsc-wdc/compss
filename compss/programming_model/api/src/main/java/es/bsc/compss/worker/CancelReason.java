package es.bsc.compss.worker;

public enum CancelReason {
    COMPSS_EXCEPTION, // When a COMPSs exception has occurred
    TIMEOUT; // When task has reached timeout
}
