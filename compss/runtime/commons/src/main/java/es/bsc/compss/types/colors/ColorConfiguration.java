package es.bsc.compss.types.colors;

import es.bsc.compss.types.exceptions.NonInstantiableException;
import es.bsc.compss.types.colors.Colors;


public class ColorConfiguration {

    public static final ColorNode[] COLORS = new ColorNode[] { 
            new ColorNode(Colors.COLOR_0, Colors.WHITE),
            new ColorNode(Colors.COLOR_1, Colors.WHITE), 
            new ColorNode(Colors.COLOR_2, Colors.BLACK),
            new ColorNode(Colors.COLOR_3, Colors.BLACK), 
            new ColorNode(Colors.COLOR_4, Colors.BLACK),
            new ColorNode(Colors.COLOR_5, Colors.WHITE), 
            new ColorNode(Colors.COLOR_6, Colors.BLACK),
            new ColorNode(Colors.COLOR_7, Colors.BLACK), 
            new ColorNode(Colors.COLOR_8, Colors.WHITE),
            new ColorNode(Colors.COLOR_9, Colors.WHITE), 
            new ColorNode(Colors.COLOR_10, Colors.BLACK),
            new ColorNode(Colors.COLOR_11, Colors.BLACK), 
            new ColorNode(Colors.COLOR_12, Colors.BLACK),
            new ColorNode(Colors.COLOR_13, Colors.WHITE), 
            new ColorNode(Colors.COLOR_14, Colors.BLACK),
            new ColorNode(Colors.COLOR_15, Colors.BLACK), 
            new ColorNode(Colors.COLOR_16, Colors.WHITE),
            new ColorNode(Colors.COLOR_17, Colors.WHITE), 
            new ColorNode(Colors.COLOR_18, Colors.BLACK),
            new ColorNode(Colors.COLOR_19, Colors.WHITE), 
            new ColorNode(Colors.COLOR_20, Colors.BLACK),
            new ColorNode(Colors.COLOR_21, Colors.WHITE), 
            new ColorNode(Colors.COLOR_22, Colors.BLACK),
            new ColorNode(Colors.COLOR_23, Colors.WHITE) 
        };

    public static final int NUM_COLORS = COLORS.length;


    private ColorConfiguration() {
        throw new NonInstantiableException("ColorConfiguration should not be instantiated");
    }

}
