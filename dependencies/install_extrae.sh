#!/bin/bash

  #
  # Displays script usage
  #
  usage() {
    echo "Usage: $0 <extraeSrcDir> <extraeTargetDir> [<tryEnableMPIMerge>]"
  }


  #
  # Setup the Extrae install environment
  #
  setup_extrae_install_environment() {
    local extraeSrc=$1

    export EXTRAE_CMD_FILE_PREFIX="extrae-cmd."
    export EXTRAE_PY_CEVENTS=0
    export CLASSPATH=$CLASSPATH:${extraeSrc}/src/java-connector/jni/
  }


  #
  # Sets MPI arguments for the Extrae installation command
  #
  # User ENV: EXTRAE_MPI_PATH, EXTRAE_MPI_HEADERS, EXTRAE_MPI_LIBS
  # Global VARS: argMpi, argMpiMerge, argMpiHeaders, argMpiLibs
  #
  set_mpi_args() {
    # Set MPI root by user or from the system
    local mpiPath
    if [ -n "${EXTRAE_MPI_PATH}" ]; then
      mpiPath="${EXTRAE_MPI_PATH}"
      argMpi="--with-mpi=${mpiPath}"
    else
      if [ -n "${I_MPI_ROOT}" ]; then
        mpiPath="${I_MPI_ROOT}"
        argMpi="--with-mpi=${mpiPath}"
        if [ -d "${mpiPath}/lib/" ]; then
          argMpiLibs="--with-mpi-libs=${mpiPath}/lib/release/"
        else
          if [ -d "${mpiPath}/lib64/" ]; then
            argMpiLibs="--with-mpi-libs=${mpiPath}/lib64/release/"
          else
            echo "ERROR: Cannot find MPI lib or lib64 folder within ${mpiPath}"
            exit 1
          fi
        fi
      else
        mpiPath=$(which mpirun 2> /dev/null)
        if [ -n "${mpiPath}" ]; then
          local mpiRealPath
          mpiRealPath=$(readlink -f "${mpiPath}")
          local mpiBaseDir=${mpiRealPath%/bin**}
          mpiPath="${mpiBaseDir}"
          argMpi="--with-mpi=${mpiPath}"
        else
          mpiPath=""
          argMpi="--without-mpi"
        fi
      fi
    fi

    # Set MPI parallel merge
    if [ -n "${mpiPath}" ]; then
      argMpiMerge="--enable-parallel-merge"
    else
      argMpiMerge=""
    fi

    # Set MPI headers by user or from the server
    if [ -n "${mpiPath}" ]; then
      if [ -n "${EXTRAE_MPI_HEADERS}" ]; then
        argMpiHeaders="--with-mpi-headers=${EXTRAE_MPI_HEADERS}"
      else
        if [ -d "${mpiPath}/include64" ]; then
          argMpiHeaders="--with-mpi-headers=${mpiPath}/include64"
        else
          if [ -d "${mpiPath}/include" ]; then
            argMpiHeaders="--with-mpi-headers=${mpiPath}/include"
          else
            echo "ERROR: Cannot automatically infer MPI include folder (${mpiPath})"
            exit 1
          fi
        fi
      fi
    else
      argMpiHeaders=""
    fi

    # Set MPI libs by user or from the system
    if [ -n "${argMpiLibs}" ]; then
      echo "WARN: MPI lbs already set in a previous step"
    else
      if [ -n "${mpiPath}" ]; then
        if [ -n "${EXTRAE_MPI_LIBS}" ]; then
          argMpiLibs="--with-mpi-libs=${EXTRAE_MPI_LIBS}"
        else
          if [ -d "${mpiPath}/lib64" ]; then
            argMpiLibs="--with-mpi-libs=${mpiPath}/lib64"
          else
            if [ -d "${mpiPath}/lib" ]; then
              argMpiLibs="--with-mpi-libs=${mpiPath}/lib"
            else
              echo "ERROR: Cannot automatically infer MPI libs folder"
              exit 1
            fi
          fi
        fi
      else
        argMpiLibs=""
      fi
    fi
  }


  #
  # Sets PAPI arguments for the Extrae installation command
  #
  # User ENV: EXTRAE_PAPI_PATH, EXTRAE_PAPI_HEADERS, EXTRAE_PAPI_LIBS
  # Global VARS: argPapi, argPapiHeaders, argPapiLibs
  #
  set_papi_args() {
    # Set PAPI root by user or from the system
    local papiPath
    if [ -n "${EXTRAE_PAPI_PATH}" ]; then
      papiPath="${EXTRAE_PAPI_PATH}"
      argPapi="--with-papi=${papiPath}"
    else
      papiPath=$(which papi_avail 2> /dev/null)
      if [ -n "$papiPath" ]; then
        local papiRealPath
        papiRealPath=$(readlink -f "${papiPath}")
        local papiBaseDir=${papiRealPath%/bin**}
        papiPath=${papiBaseDir}
        argPapi="--with-papi=${papiPath}"
      else
        papiPath=""
        argPapi="--without-papi"
      fi
    fi

    # Set PAPI headers by user or from the system
    if [ -n "${papiPath}" ]; then
      if [ -n "${EXTRAE_PAPI_HEADERS}" ]; then
        argPapiHeaders="--with-papi-headers=${EXTRAE_PAPI_HEADERS}"
      else
        argPapiHeaders="--with-papi-headers=${papiPath}/include"
      fi
    else
      argPapiHeaders=""
    fi

    # Set PAPI headers by user or from the system
    if [ -n "${papiPath}" ]; then
      if [ -n "${EXTRAE_PAPI_LIBS}" ]; then
        argPapiLibs="--with-papi-libs=${EXTRAE_PAPI_LIBS}"
      else
        if [ -d "${papiPath}/lib64" ]; then
          argPapiLibs="--with-papi-libs=${papiPath}/lib64"
        else
          if [ -d "${papiPath}/lib" ]; then
            argPapiLibs="--with-papi-libs=${papiPath}/lib"
          else
            echo "ERROR: Cannot automatically infer PAPI libs folder"
            exit 1
          fi
        fi
      fi
    else
      argPapiLibs=""
    fi
  }


  #
  # Commands to install Extrae
  #
  # Global VARS used: argMpi, argMpiMerge, argMpiHeaders, argMpiLibs, argPapi, argPapiHeaders, argPapiLibs
  # ENV used: JAVA_HOME
  #
  install() {
    local extraeSrc=$1
    local extraeTarget=$2

    # Move to sources directory
    cd "${extraeSrc}" || exit 1

    # Configure, compile and install
    autoreconf --force --install
    ev=$?
    if [ "$ev" -ne 0 ]; then
      exit $ev
    fi

    if [ $(uname) == "Darwin" ]; then   # para linux parece que si que es necesario tambien en Shaheen.
      otherFlags="--enable-pthread"
    else
      otherFlags=""
    fi

    is_cray="false"
    if test -d /opt/cray ; then
       if test `which cc | grep xt-asyncpe | wc -l` != "0" ; then
         is_cray="true"
       elif test `which cc | grep craype | wc -l` != "0" ; then
         is_cray="true"
       fi
    fi

    if [ "${is_cray}" == "false" ]; then
      # No Cray machine
      ./configure \
        --enable-gettimeofday-clock \
        --without-unwind \
        --without-dyninst \
        --without-binutils \
        "${argMpi}" "${argMpiMerge}" "${argMpiHeaders}" "${argMpiLibs}" \
        "${argPapi}" "${argPapiHeaders}" "${argPapiLibs}"\
        --with-java-jdk="${JAVA_HOME}" \
        --disable-openmp \
        --enable-nanos \
        --disable-smpss \
        --disable-instrument-io \
        --disable-pebs-sampling \
        --disable-pthread-cond-calls \
        "${otherFlags}" \
        --prefix="${extraeTarget}" \
        --libdir="${extraeTarget}/lib" \
        --target=${TARGET_HOST}
      ev=$?
    else
      # Cray machine
      ./configure \
        --enable-pthread \
        --enable-gettimeofday-clock \
        --without-unwind \
        --without-dyninst \
        --without-binutils \
        "${argMpi}" "${argMpiMerge}" "${argMpiHeaders}" "${argMpiLibs}" \
        "${argPapi}" "${argPapiHeaders}" "${argPapiLibs}"\
        --with-java-jdk="${JAVA_HOME}" \
        --disable-openmp \
        --enable-nanos \
        --disable-smpss \
        --disable-instrument-io \
        --disable-pebs-sampling \
        --prefix="${extraeTarget}" \
        --libdir="${extraeTarget}/lib" \
        --disable-xmltest \
        --with-binary-type=64 \
        --host=x86_64-linux-gnu \
        --target=x86_64-linux-gnu \
        --with-xml-prefix=/usr \
        CC=cc CFLAGS='-O3 -g -std=gnu90 -lpthread' LDFLAGS='-O3 -g -std=gnu90 -lpthread' CXX=CC CXXFLAGS='-O3 -g' F77=ftn FFLAGS='-O3 -g -std=gnu90 -lpthread'
      ev=$?
    fi

    if [ "$ev" -ne 0 ]; then
      exit $ev
    fi

    make clean install
    ev=$?
    if [ "$ev" -ne 0 ]; then
      exit $ev
    fi
  }


  #
  # MAIN METHOD
  #
  main() {
    # Retrieve script variables
    if [ "$#" -lt 2 ]; then
       echo "ERROR: Invalid number of parameters"
       usage
       exit 1
    fi
    extraeSrcDir=$1
    extraeTargetDir=$2
    tryEnableMPIMerge=${3:-false}

    echo "Install script parameters:"
    echo " * Extrae Source Dir: ${extraeSrcDir}"
    echo " * Extrae Target Dir: ${extraeTargetDir}"
    echo " * Try Enable MPI Merge: ${tryEnableMPIMerge}"

    # Build Extrae MPI args
    if [ -n "${tryEnableMPIMerge}" ] && [ "${tryEnableMPIMerge}" == "false" ]; then
      argMpi="--without-mpi"
      argMpiMerge=""
      argMpiHeaders=""
      argMpiLibs=""
    else
      set_mpi_args
    fi

    echo "Extrae MPI args:"
    echo " * ${argMpi}"
    echo " * ${argMpiMerge}"
    echo " * ${argMpiHeaders}"
    echo " * ${argMpiLibs}"


    # Build Extrae PAPI args
    set_papi_args

    echo "Extrae PAPI args:"
    echo "* ${argPapi}"
    echo "* ${argPapiHeaders}"
    echo "* ${argPapiLibs}"

    # Setup extrae environment
    setup_extrae_install_environment "${extraeSrcDir}"

    # Create installation folder
    mkdir -p "${extraeTargetDir}" || exit 1

    # Move to Extrae sources folder
    cd "${extraeSrcDir}" || exit 1

    # Install
    install "${extraeSrcDir}" "${extraeTargetDir}"

    # Exit all ok
    echo "EXTRAE successfully installed!"
    exit 0
  }


  #
  # ENTRY POINT
  #
  main "$@"
