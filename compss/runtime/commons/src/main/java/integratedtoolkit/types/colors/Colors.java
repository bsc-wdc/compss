package integratedtoolkit.types.colors;

import integratedtoolkit.types.exceptions.NonInstantiableException;


public class Colors {

    public static final String BLACK = "#000000";
    public static final String WHITE = "#ffffff";

    // Consistent with PARAVER default colors
    public static final String COLOR_0 = String.format("#%02x%02x%02x", 117, 195, 255);
    public static final String COLOR_1 = String.format("#%02x%02x%02x", 0, 0, 255);
    public static final String COLOR_2 = String.format("#%02x%02x%02x", 255, 255, 255);
    public static final String COLOR_3 = String.format("#%02x%02x%02x", 255, 0, 0);
    public static final String COLOR_4 = String.format("#%02x%02x%02x", 255, 0, 174);
    public static final String COLOR_5 = String.format("#%02x%02x%02x", 179, 0, 0);
    public static final String COLOR_6 = String.format("#%02x%02x%02x", 0, 255, 0);
    public static final String COLOR_7 = String.format("#%02x%02x%02x", 255, 255, 0);
    public static final String COLOR_8 = String.format("#%02x%02x%02x", 235, 0, 0);
    public static final String COLOR_9 = String.format("#%02x%02x%02x", 0, 162, 0);
    public static final String COLOR_10 = String.format("#%02x%02x%02x", 255, 0, 255);
    public static final String COLOR_11 = String.format("#%02x%02x%02x", 100, 100, 177);
    public static final String COLOR_12 = String.format("#%02x%02x%02x", 172, 174, 41);
    public static final String COLOR_13 = String.format("#%02x%02x%02x", 255, 144, 26);
    public static final String COLOR_14 = String.format("#%02x%02x%02x", 2, 255, 177);
    public static final String COLOR_15 = String.format("#%02x%02x%02x", 192, 224, 0);
    public static final String COLOR_16 = String.format("#%02x%02x%02x", 66, 66, 66);
    public static final String COLOR_17 = String.format("#%02x%02x%02x", 255, 0, 96);
    public static final String COLOR_18 = String.format("#%02x%02x%02x", 169, 169, 169);
    public static final String COLOR_19 = String.format("#%02x%02x%02x", 169, 0, 0);
    public static final String COLOR_20 = String.format("#%02x%02x%02x", 0, 109, 255);
    public static final String COLOR_21 = String.format("#%02x%02x%02x", 200, 61, 68);
    public static final String COLOR_22 = String.format("#%02x%02x%02x", 200, 66, 0);
    public static final String COLOR_23 = String.format("#%02x%02x%02x", 0, 41, 0);


    public Colors() {
        throw new NonInstantiableException("Colors should not be instantiated");
    }

}
