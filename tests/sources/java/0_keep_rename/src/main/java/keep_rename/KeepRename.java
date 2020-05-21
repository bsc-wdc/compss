package keep_rename;

public class KeepRename {

    /**
     * Test main entry.
     * 
     * @param args System arguments.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String content = "content";
        String file = KeepRenameImpl.FILENAME;

        KeepRenameImpl.writeFileKeepRename(file, content);
        KeepRenameImpl.readFileNoRename(file, content);

        /*
         * COLLECTIONS not supported in JAVA List<String> files = new ArrayList<String>();
         * KeepRenameImpl.writeListKeepRename(files, content); KeepRenameImpl.readListNoRename(files, content);
         */
    }

}
