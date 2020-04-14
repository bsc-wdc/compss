# Displays info messages
display_info() {
  local msg=$1
  echo "[  INFO] ${msg}"
}

# Displays warning messages
display_warning() {
  local msg=$1
  echo "[ WARNING ]: ${msg}"
}

# Displays parsing arguments errors
display_error() {
  local error_msg=$1

  echo " "
  echo "$error_msg"
  echo " "

}

# Displays runtime/application errors
fatal_error() {
  local error_msg=$1
  local error_code=$2

  # Display error
  display_error "${error_msg}"

  # Exit
  exit "${error_code}"
}
