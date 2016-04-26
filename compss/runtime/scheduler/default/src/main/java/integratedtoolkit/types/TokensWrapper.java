package integratedtoolkit.types;

public class TokensWrapper {

    private int tokens;

    public TokensWrapper(int tokens) {
        this.tokens = tokens;
    }

    public int getFree() {
        return tokens;
    }

    public void removeTask() {
        tokens++;
    }

    public void addTask() {
        tokens--;
    }
}
