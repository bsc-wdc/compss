package keep_rename;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface KeepRenameItf {

    @Method(declaringClass = "keep_rename.KeepRenameImpl")
    public void writeFileKeepRename(
        @Parameter(type = Type.FILE, direction = Direction.OUT, keepRename = true) String file,
        @Parameter(type = Type.STRING, direction = Direction.IN) String content);

    @Method(declaringClass = "keep_rename.KeepRenameImpl")
    public void readFileNoRename(@Parameter(type = Type.FILE, direction = Direction.IN, keepRename = false) String file,
        @Parameter(type = Type.STRING, direction = Direction.IN) String content);

    /*
     * COLLECTIONS NOT SUPPORTED YET
     * 
     * @Method(declaringClass="keep_rename.KeepRenameImpl") public void writeListKeepRename(
     * 
     * @Parameter(type=Type.COLLECTION_FILE, direction=Direction.OUT, keepRename = true) List<String> file,
     * 
     * @Parameter(type=Type.STRING, direction= Direction.IN) String content);
     * 
     * public static void readListNoRename(List<String> files, String content)
     */

}
