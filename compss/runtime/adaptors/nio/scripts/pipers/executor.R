# Check if the correct number of command-line arguments is provided
if (length(commandArgs(trailingOnly = TRUE)) != 2) {
  cat("Usage: Rscript script.R input_fifo output_fifo\n")
  q("no")
}

# Extract input and output FIFO paths from command-line arguments
input_fifo_path <- commandArgs(trailingOnly = TRUE)[1]
output_fifo_path <- commandArgs(trailingOnly = TRUE)[2]

# Open the input FIFO for reading
input_fifo <- fifo(input_fifo_path, open = "r", blocking=TRUE)

# Open the output FIFO for writing
output_fifo <- fifo(output_fifo_path, open = "w+", blocking=TRUE)

# Read from input FIFO and write to output FIFO
while (TRUE) {
  # Read data from input FIFO
  data <- readLines(input_fifo, n = 1)
  cat("Received:", data, "\n")  
  # Check if data is empty (end of stream)
  if (length(data) == 0) {
    break
  }
  
  # Check if the received data is "QUIT" and exit the loop
  if (data == "QUIT") {
    break
  }

  split_data <- strsplit(data, " ")[[1]]
  tag <- split_data[1]
  if (tag == "EXECUTE_TASK"){
    # Print received data to the console
    task_id <- split_data[2] #(int)
    sandbox <- split_data[3]
    job_out <- split_data[4]
    job_err <- split_data[5]
    tracing <- split_data[6] #bool
    #task_id <- split_data[7] 
    debug <- split_data[8] #bool
    #storage_conf <- split_data[9]
    #task_type <- split_data[10]
    module <- split_data[11]
    func <- split_data[12]
    #time_out = split_data[13] #int
    params <- split_data[14:length(split_data)]
    #other params [ number_nodes [node_names] num_threads(int) has_target(bool) return_type(type|null) num_returns num_args [args: type(int) stdio_stream prefix name value] [target: same as before] [returns]    

    cat("Received task execution with id: ", task_id, ", module: ", module, ", function: ", func, " params: ", params, "\n")  
    # TODO: Add here the process of the task and write end_task message
    # Writing "END_TASK" message at this m  failed task
    # Load the module
    tryCatch(
	{
		source(module)
    		# Call the function using get()
    		if (exists(func)) {
       			result <- get(func)()
       			print(result)
			# TODO: Manage returns
			cat("END_TASK", task_id, 0, file = output_fifo, "\n")
    		} else {
       			cat("Function", function_name, "does not exist")
			cat("END_TASK", task_id, 1, file = output_fifo, "\n")
		}
	}, error = function(e) {
    		# Handle the error
    		print(paste("Error:", e$message))
		traceback()	
		cat("END_TASK", task_id, 1, file = output_fifo, "\n")
	})

    cat("END_TASK", task_id, 1, file = output_fifo, "\n")
  } else {
    cat("Received:", data, "\n")
  }
}

# Close the FIFOs
close(input_fifo)
close(output_fifo)

