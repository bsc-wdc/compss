#!/bin/bash
cd "$(dirname "$0")"

AUTO_CBM_DIR=../..
cd $AUTO_CBM_DIR

PREF="[Auto CBM] -"
MN_USERNAME="$1" #bscxxxx
MN_LOGIN="$2"    #mnx.bsc.es


if [ -z $MN_USERNAME ] || [ -z $MN_LOGIN ] ; then
        echo "USAGE: ./get-results MN_USERNAME MN_LOGIN"
        echo "Please specify first your Marenostrum USERNAME(bscxxxx) and then the LOGIN NODE (mnx.bsc.es)."
        exit -1
fi

function finish
{
	mv auto-cbm/cbm* . &> /dev/null
        rm -r auto-cbm &> /dev/null
        echo ; exit
}
trap finish SIGHUP SIGINT EXIT


echo ":::::::::::::::::::::::::::::::"
echo ":::::::::: Auto CBM :::::::::::"
echo ":::::::::::::::::::::::::::::::"


echo "$PREF Compiling JARs..."
mvn clean package
if [ "$?" != "0" ] ; then exit 1 ; fi  # Check for failure

# Move the older one to an old one. Create auto-cbm empty dir. 
echo "$PREF Copying auto-cbm to '$MN_USERNAME@$MN_LOGIN:~/'"
ssh "$MN_USERNAME@$MN_LOGIN" "rm -r ~/auto-cbm.old ; \ 
			      mv ~/auto-cbm ~/auto-cbm.old; "

#  Create auto-cbm directory in Marenostrum
mkdir ./auto-cbm
cp -r ./apps ./scripts ./auto-cbm
rm -rf ./auto-cbm/scripts/get-results
scp -pr ./auto-cbm "$MN_USERNAME@$MN_LOGIN:~"
rm -r ./auto-cbm
#

# Clears must be done after every benchmark to avoid quota exceeding
echo "$PREF Executing enqueue scripts..."

CLEAN_DATASETS_INSTRUCTION="\
sleep 7 ; jobID=\$(bjobs | tail -n 1 | cut -c -7) ; \
echo -e \"\$(../scripts/launch/create-bsub-to-clear-datasets.sh \$jobID)\" > tmp.bsub; \
bsub < tmp.bsub ; rm tmp.bsub ; "

NOTIFY_JENKINS_INSTRUCTION="\
sleep 7 ; jobID=\$(bjobs | tail -n 1 | cut -c -7) ; \
echo -e \"\$(../scripts/launch/create-bsub-to-notify-jenkins-by-file.sh \$jobID)\" > tmp.bsub; \
bsub < tmp.bsub ; rm tmp.bsub ; "

ssh "$MN_USERNAME@$MN_LOGIN" \
	"\
		cd ~/auto-cbm/apps ; \
		\
		cd cbm1 ; ./enq*1.sh ; cd .. ; $CLEAN_DATASETS_INSTRUCTION \
		cd cbm2 ; ./enq*2.sh ; cd .. ; $CLEAN_DATASETS_INSTRUCTION \
		cd cbm3 ; ./enq*3.sh ; cd .. ; $CLEAN_DATASETS_INSTRUCTION \
		\
		cd matmul ; ./enq*matmul.sh ; cd .. ; $CLEAN_DATASETS_INSTRUCTION \
		cd kmeans ; ./enq*kmeans.sh ; cd .. ; $CLEAN_DATASETS_INSTRUCTION \
		\
		$NOTIFY_JENKINS_INSTRUCTION
		\
	"
echo "$PREF Finished enqueuing"

