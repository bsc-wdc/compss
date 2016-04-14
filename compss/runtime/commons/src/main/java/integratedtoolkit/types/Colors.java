package integratedtoolkit.types;

import integratedtoolkit.types.exceptions.NonInstantiableException;


public class Colors {

    public static final String BLACK = "#000000";
    public static final String DARK_BLUE = "#0000ff";
    public static final String LIGHT_GREEN = "#00ff00";
    public static final String LIGHT_BLUE = "#00ffff";
    public static final String VIOLET = "#6600ff";
    public static final String DARK_RED = "#990000";
    public static final String PURPLE = "#9900ff";
    public static final String BROWN = "#996600";
    public static final String DARK_GREEN = "#999900";
    public static final String GREY = "#c0c0c0";
    public static final String RED = "#ff0000";
    public static final String PINK = "#ff00ff";
    public static final String YELLOW = "#ffff00";
    public static final String WHITE = "#ffffff";

    private Colors() {
        throw new NonInstantiableException("Colors should not be instantiated");
    }

}
