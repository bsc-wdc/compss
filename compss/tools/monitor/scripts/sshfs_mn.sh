#!/bin/bash -e


#########################################
#            FUNCTIONS                  #
#########################################
  
usage() { 
  exitValue=$1

  /bin/cat <<EOT
Usage: $0 [options] [local_target mn_user]

* Options:
    --help, -h                              Print this help message
    --install, -i			    Install
    --configure, -c	 		    Configure
    --mount, -m				    Mount
    --umount, -u			    Umount

*   local_taget				    Local target folder to mount MN sshfs
				 	    Needed for configure, mount and umount options

*   mn_user				    Marenostrum username
					    Needed for configure and mount options

EOT
  exit $exitValue
}

# Displays parsing arguments errors
display_error() {
  local error_msg=$1

  echo $error_msg
  echo " "

  usage 1
}


get_args() {
  #Parse Options
  while getopts hicmu-: flag; do
    # Treat the argument
    case "$flag" in
      h)
        # Display help
        usage 0
        ;;
      i)
        # Install
        install=true
        ;;
      c)
        # Configure
        configure=true
        ;;
      m)
        # Mount
        mount=true
        ;;
      u)
        # Umount
        umount=true
        ;;
      -)
        # Check more complex arguments 
        case "$OPTARG" in
          help)
            # Display help
            usage 0
            ;;
          install)
            install=true
            ;;
          configure)
	    configure=true
            ;;
          mount)
            mount=true
            ;;
          umount)
            umount=true
            ;;
          *)
            # Flag didn't match any patern. Raise exception 
            display_error "Bad argument: $OPTARG"
            ;;
        esac
        ;;
      *)
        # Flag didn't match any patern. End of flags
        break
        ;;
    esac
  done
  #Shift script arguments
  shift $((OPTIND-1))

  # Get rest of parameters
  if [ $# -ne 0 ]; then
    # Target location
    local_folder=$1
    shift
  fi

  if [ $# -ne 0 ]; then
    # MN User
    mn_user=$1
    shift
  fi
}

check_args() {
  # Check local_folder
  if [ "$mount" == "true" ] || [ "$umount" == "true" ]; then
    if [ "${local_folder}" == "" ]; then
      display_error "Bad argument: empty local folder"
    fi
  fi

  # Check mn_user
  if [ "$configure" == "true" ] || [ "$mount" == "true" ]; then
    if [ "${mn_user}" == "" ]; then
      display_error "Bad argument: empty mn_user"
    fi
  fi

}

proc_requests() {
  # Install sshfs
  if [ "$install" == "true" ]; then
    echo "* Installing SSHFS..."
    distr=$(lsb_release -si)
    if [ "$distr" == "ubuntu" ]; then
      sudo apt-get install sshfs
      echo "* SSHFS installed!"
    elif [ "$distr" == "openSUSE project" ]; then
      sudo zypper install sshfs
      echo "* SSHFS installed!"
    else 
      display_error "Unknown distribution. Cannot install sshfs"
    fi
  fi

  # Ensure ssh passwordless connection to mn1
  if [ "$configure" == "true" ]; then
    echo "* Configuring MN access..."
    scp $HOME/.ssh/id_dsa.pub ${mn_user}@mn1.bsc.es:~/id_dsa_local.pub
    ssh ${mn_user}@mn1.bsc.es "cat ~/id_dsa_local.pub >> ~/.ssh/authorized_keys; rm ~/id_dsa_local.pub"
    echo "* MN access configured!"
  fi

  # Mount sshfs
  if [ "$mount" == "true" ]; then
    # Create local folder
    echo "* Creating local folder..."
    mkdir -p ${local_folder}/.COMPSs
    echo "* Local folder created!"
    # Mount sshfs
    echo "* Mounting sshfs..."
    sshfs -o IdentityFile=$HOME/.ssh/id_dsa -o allow_other ${mn_user}@mn1.bsc.es:~/.COMPSs ${local_folder}/.COMPSs
    echo "* sshfs mounted!"
    echo "  Now you are ready to use COMPSs Monitor"
  fi

  # Umount sshfs
  if [ "$umount" == "true" ]; then
    echo "* Umount sshfs"
    sudo umount ${local_folder}/.COMPSs
    echo "* sshfs directory removed!"
  fi
}



#########################################
#             MAIN PROGRAM              #
#########################################

  # Init script variables
  install=false
  configure=false
  mount=false
  umount=false
  local_folder=""
  mn_user=""

  # Display warning message
  echo "####################################################"
  echo "# WARNING: DURING THIS PROCESS YOU CAN BE ASKED    #"
  echo "#          SERVERAL TIMES FOR YOUR MN ACCOUNT      #"
  echo "#          PASSWORD OR FOR YOUR ROOT PASSWORD.     #"
  echo "####################################################"
  echo " "
  sleep 2

  # Get parameters
  get_args $*

  # Check parameters
  check_args

  # Process requests
  proc_requests

