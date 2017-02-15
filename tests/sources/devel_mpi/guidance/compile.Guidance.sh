#######################################################################
#                                                                     #
# This script is used to compile Guidance software.                   #
#                                                                     #
#                                                                     #
# IT_HOME:  The environment variable containg the path of the         #
#           COMPSs framework installation.                            #
#           You can verify this in your system by doing               #
#           (in the command line):                                    #
#                                                                     #
#           > echo $IT_HOME                                           #
#                                                                     #
#                                                                     #
# JAVA_COMPILER: The environment variable containing the path         #
#                of the compiler.                                     #
#                YOU MUST change it according to your system.         #
#                                                                     #
#                IMPORTANT: The compilation has been tested with      #
#                the following Java versions:                         #
#                                                                     #
#                  - jdk1.8                                           #
#                  -                                                  #
#                  -                                                  #
#                                                                     #
#                Make sure you are using one of them.                 #
#                                                                     #
#                                                                     #
# SOURCEPATH: The environment variable containing the path of         #
#             Guidance source code.                                   #
#             You can change it according to your preferences.        #
#                                                                     #
#                                                                     #
#######################################################################


export CLASSPATH=$CLASSPATH:$IT_HOME/compss-engine.jar

# Change the following variables according to your system.
#SOURCEPATH="/home/bsc05/bsc05025/TOOLS/MN3/guidance_0.9.8"
SOURCEPATH="/gpfs/projects/bsc19/guidance_0.9.8"
#SOURCEPATH="/home/compss/workspace_java/guidance_0.9.8"

javac -sourcepath ${SOURCEPATH} guidance/*.java

jar cf guidance.jar guidance

# Uncoment this line to create html and latex documentation using doxygen
# First, verify that you have doxygen installed in your system

#doxygen Doxyfile
