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


    cat("The module is:", module, "\n")
    tryCatch(
             {
               source(module)
               cat("Finished     source(module)", file = job_out)
               # Call the function using get()
               if (exists(func)) {

                 num_of_nodes <- as.integer(params[1])
                 cat("num_of_nodes is: ", num_of_nodes)
                 num_of_threads <- as.integer(params[1+num_of_nodes+1])
                 has_target <- as.logical(params[1+num_of_nodes+2]) #!!!
                 return_type <- params[1+num_of_nodes+3]
                 num_of_returns <- as.integer(params[1+num_of_nodes+4])
                 num_of_args <- as.integer(params[1+num_of_nodes+5])
                 cat("num_of_args is", num_of_args)
                 #result <- get(func)(3, 4)
                 leng_params_func <- length(formalArgs(func))
                 # print(formalArgs(func))
                 cat("leng_params_func is", leng_params_func)
                 params_func_list <- list(leng_params_func)

                 first_arg_ind <- num_of_nodes + 7 
                 for(i in 0:(leng_params_func-1)){
                   if(params[first_arg_ind] == "0"){
                     params_func_list[[i+1]] <- as.integer(params[first_arg_ind + 5])
                     names(params_func_list)[i+1] <- params[first_arg_ind + 3]
                     first_arg_ind <- first_arg_ind + 6
                   }else if(params[first_arg_ind] == "7"){
                     params_func_list[[i+1]] <- as.numeric(params[first_arg_ind + 5])
                     names(params_func_list)[i+1] <- params[first_arg_ind + 3]
                     first_arg_ind <- first_arg_ind + 6
                   }else if(params[first_arg_ind] == "8"){
                     num_of_words <- as.integer(params[first_arg_ind + 5])
                     vec_words <- character(num_of_words)
                     for(j in 1:num_of_words){
                       vec_words[j] <- as.character(params[first_arg_ind + 5 + j])
                     }
                     params_func_list[[i+1]] <- paste0(vec_words, collapse = " ")
                     names(params_func_list)[i+1] <- params[first_arg_ind + 3]
                     first_arg_ind <- first_arg_ind + 6 + num_of_words
                   }else if(params[first_arg_ind] == "10"){
                     content_type <- as.character(params[first_arg_ind + 4])
                     # print(paste0("The content type is: ", content_type, "\n"))
                     path_value <- as.character(params[first_arg_ind + 5])
                     path_value <- strsplit(path_value, ":")[[1]]
                     path_value <- path_value[length(path_value)]
                     if(content_type != "null"){
                       # print(paste0("The path value is: ", path_value, "\n"))
                       #params_func_list[[i+1]] <- compss_unserialize(filepath = path_value)
                       #params_func_list[[i+1]] <- readRDS(file = path_value)
                       ext <- strsplit(path_value, "[.]")[[1]]
                       ext <- ext[length(ext)]
                       con <- file(description = path_value, open = "rb")
                       par_raw <- readBin(con, what = raw(), n = file.info(path_value)$size)
                       params_func_list[[i+1]] <- unserialize(connection = par_raw)
                       close(con)
                       # print("params_func_list\n")
                       # print(params_func_list[i+1])
                     }else{
                       params_func_list[[i+1]] <- path_value
                     }
                     names(params_func_list)[i+1] <- params[first_arg_ind + 3]
                     first_arg_ind <- first_arg_ind + 6
                   }else{
                     cat("i = ", i)
                     cat("Non-supported type: ", params[first_arg_ind + 6*i])
                   }
                 }
                 # print("params_func_list:\n")
                 # print(params_func_list)
                 result <- do.call(func, params_func_list)
                 cat("num_of_returns:", num_of_returns, "\n")
                 if(num_of_returns > 0){
                   path_return_value <- as.character(params[first_arg_ind + 5])
                   path_return_value <- strsplit(path_return_value, ":")[[1]]
                   path_return_value <- path_return_value[length(path_return_value)]
                   # compss_serialize(object = result, filepath = path_return_value)
                   con <- file(description = path_return_value, open = "wb")
                   x <- serialize(object = result, connection = NULL)
                   writeBin(x, con)
                   close(con)
                   cat("The path is: ", path_return_value)
                 }
                 # print("The results is:\n")
                 # print(result)
                 cat("END_TASK", task_id, 0, file = output_fifo, "\n")
               } else {
                 cat("Function", function_name, "does not exist")
                 cat("END_TASK", task_id, 1, file = output_fifo, "\n")
               }
             }, error = function(e) {
               # Handle the error
               cat("Error:", e$message, file = job_err)
               print(paste("Error:", e$message))
               traceback()	
               cat("END_TASK", task_id, 1, file = output_fifo, "\n")
             })

    # cat("END_TASK", task_id, 1, file = output_fifo, "\n")
  } else {
    cat("Received:", data, "\n")
  }
}

# Close the FIFOs
close(input_fifo)
close(output_fifo)
#
