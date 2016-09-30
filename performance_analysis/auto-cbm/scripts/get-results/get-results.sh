#!/bin/bash
cd "$(dirname $0)"


AUTO_CBM_DIR="$(realpath "../..")"
RES_DIR="$AUTO_CBM_DIR/results"

APP_NAMES=( "cbm1" "cbm2" "cbm3" "matmul" "kmeans" )

MN_USERNAME="$1" # bscxxxx
MN_LOGIN="$2"    # mnx.bsc.es

if [ -z $MN_USERNAME ] || [ -z $MN_LOGIN ] ; then
	echo "USAGE: ./get-results MN_USERNAME MN_LOGIN"
	echo "Please specify first your Marenostrum USERNAME(bscxxxx) and then the LOGIN NODE (mnx.bsc.es)."
	exit -1
fi

function finish { 
  echo ; exit -1 
}
trap finish SIGHUP SIGINT EXIT

PREF="[Auto CBM] -"
echo ":::::::::::::::::::::::::::::::"
echo ":::::::::: Auto CBM :::::::::::"
echo ":::::::::::::::::::::::::::::::"
echo

result=""
while [ -z "$result" ] 
do
	echo "$PREF Waiting for cbm to finish..."
	result=$(ssh "$MN_USERNAME@$MN_LOGIN" "cd ~/auto-cbm/apps; ls -1 | grep cbm-finished ;" 2> /dev/null)
		
	if [ -z "$result" ] ; then 
		echo "$PREF Nothing found. Retrying in a while..."
		sleep 30
	fi
done
echo "$PREF Auto-cbm has finished!!!"
#########


echo "$PREF Removing .COMPSs tmpFiles... (this could take a while)"
ssh "$MN_USERNAME@$MN_LOGIN" "\
     rm -rf ~/.COMPSs/cbm*/tmpFiles &> /dev/null ; " &> /dev/null

echo "$PREF Cleaning former executions..."
rm -rf $RES_DIR
mkdir -p $RES_DIR

echo "$PREF Retrieving files..."
for APP in ${APP_NAMES[@]}
do
  echo "$PREF Retrieving files for ${APP}"
  mkdir -p $RES_DIR/${APP}-data &> /dev/null
  mkdir -p $RES_DIR/${APP}-debug &> /dev/null
  scp "$MN_USERNAME@$MN_LOGIN:~/auto-cbm/apps/${APP}/*.out" $RES_DIR/${APP}-data  &> /dev/null
  scp -r "$MN_USERNAME@$MN_LOGIN:~/.COMPSs/${APP}*"    $RES_DIR/${APP}-debug &> /dev/null
done


echo "$PREF Removing .COMPSs"
ssh "$MN_USERNAME@$MN_LOGIN" "\
     rm -rf ~/.COMPSs/cbm* &> /dev/null ; " &> /dev/null
     
echo "$PREF Processing results"
./cbmi/process-cbmi-results.sh
./others/save-csv*.sh

echo "$PREF Finished. Check '$RES_DIR' to see the results."
