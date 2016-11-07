package integratedtoolkit.types.colors;

public class ColorNode {

    private final String fillColor;
    private final String fontColor;


    public ColorNode(String fillColor, String fontColor) {
        this.fillColor = fillColor;
        this.fontColor = fontColor;
    }

    public String getFillColor() {
        return this.fillColor;
    }

    public String getFontColor() {
        return this.fontColor;
    }

}