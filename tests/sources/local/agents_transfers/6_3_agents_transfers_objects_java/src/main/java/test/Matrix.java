package test;

/**
 * Tests transfers between agents.
 */
public class Matrix implements java.io.Serializable {

    int[][] content;


    public Matrix() {
        this.content = new int[0][0];
    }

    public Matrix(int cols, int rows, int val) {
        this.content = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                this.content[i][j] = val;
            }
        }
    }

    public void setVal(int val) {
        for (int i = 0; i < this.content.length; i++) {
            for (int j = 0; j < this.content[i].length; j++) {
                this.content[i][j] = val;
            }
        }
    }

    public int getVal() {
        return content[0][0];
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.content.length; i++) {
            for (int j = 0; j < this.content[i].length; j++) {
                sb.append(this.content[i][j]);
                sb.append(' ');
            }
            sb.append('\n');
        }
        return sb.toString();
    }

}
