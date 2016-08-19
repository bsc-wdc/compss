package testMPI.types;


public class MPIExecutionResult {

	private int exitValue;
	private String errorMessage;
	private String outputMessage;

	
	public MPIExecutionResult() {
		this.exitValue = -1;
		this.errorMessage = "";
		this.outputMessage = "";
	}

	public int getExitValue() {
		return exitValue;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public String getOutputMessage() {
		return outputMessage;
	}
	
	public String getValueFromOutput() {
		String[] lines = outputMessage.split("\n");
		String value = lines[lines.length - 1];
		return value;
	}

	public void setExitValue(int exitValue) {
		this.exitValue = exitValue;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public void setOutputMessage(String outputMessage) {
		this.outputMessage = outputMessage;
	}

}
