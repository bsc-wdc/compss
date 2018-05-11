package es.bsc.compss.types;

import java.io.File;


public class BindingObject {
    private String id;
    private int type;
    private int elements;
    
    public BindingObject(String id, int type, int elements) {
        this.id = id;
        this.type = type;
        this.elements = elements;
    }
    
    public String getId() {
        return id;
    }
    
    public String getName(){
        int index = id.lastIndexOf(File.separator);
        if (index>0){
            return id.substring(index+1);
        }else{
            return id;
        }
    }
    
    public int getType() {
        return type;
    }

    
    public int getElements() {
        return elements;
    }
    
    public static BindingObject generate(String path){
        String[] extObjVals = path.split("#");
        //id = extObjVals[0].substring(extObjVals[0].lastIndexOf(File.pathSeparator)+1);
        String id = extObjVals[0];
        int type = Integer.parseInt(extObjVals[1]);
        int elements = Integer.parseInt(extObjVals[2]);
        return new BindingObject(id, type, elements);
    }
    
    @Override
    public String toString(){
        return id+"#"+type+"#"+elements;
    }
    
    
}
