package es.bsc.compss.types.annotations.parameter;

/**
 * Parameter types (for bindings and internal java)
 *
 */
public enum DataType {
    BOOLEAN_T, 
    CHAR_T, 
    BYTE_T, 
    SHORT_T, 
    INT_T, 
    LONG_T, 
    FLOAT_T, 
    DOUBLE_T, 
    STRING_T,           // Java: String , Bindings: String / small serialized object
    FILE_T,             // Java: File , Bindings: File / Object serialized
    OBJECT_T,           // Java: OBJ / SCO , Bindings: does not exist
    PSCO_T,             // Java: PSCOs
    EXTERNAL_OBJECT_T   // Bindings: Objects (currently, PSCOs)
}


