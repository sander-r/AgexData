#!/usr/bin/env bash

# Run Example: bash runner.sh --mode AE --directories AA BB CC DD EE FF
# Note: --mode parameter must go first in order to make the script behave correctly

while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -d|--directories)
    DIRECTORIES="${*:2}"
    shift
    ;;
    -m|--mode)
    MODE="$2"
    shift
    ;;
esac
shift
done

if [ $MODE == "AAA" ]; then
    SCRIPT_NAME="ScanStatus_AAA.py"
    XLS_FILE="AAADB_Valid.xlsx"
    DEST_PATH="/Volumes/dhl/Files/VirtualSlides/AAA-SLIDES/"
    #DEST_PATH="/home/fran/Escritorio/Script_general/Files/VirtualSlides/AAA-SLIDES/"
elif [ $MODE == "AE" ]; then
    SCRIPT_NAME="ScanStatus_AE.py"
    XLS_FILE="AEDB_Valid.xlsx"
    DEST_PATH="/Volumes/dhl/Files/VirtualSlides/AE-SLIDES/"
    #DEST_PATH="/home/fran/Escritorio/Script_general/Files/VirtualSlides/AE-SLIDES/"
fi

BASE_DIR=$(pwd)
# SCRIPT_NAME=$(ls |grep *py)
# XLS_FILE=$(ls |grep *.xlsx)
DIREXCLUDE="UNKNOWN _administration _bl_ps"

if [[ ! $DIRECTORIES ]]; then

    #DIRECTORIES=$(ls -d */ | cut -f1 -d'/')
    DIRECTORIES=$(ls -p $DEST_PATH | grep "/" | cut -f1 -d'/')

    for EXCLUDE in $DIREXCLUDE
    do
        DIRECTORIES=`echo $DIRECTORIES | sed "s/$EXCLUDE//g"`
    done
fi

for dir in $DIRECTORIES
do
    CUR_DIR=$DEST_PATH$dir
    cd $CUR_DIR

    echo
    echo "######################"
    echo "# Now scanning "
    echo "######################"
    echo

    if [ -f $CUR_DIR/_ADMINISTRATION/status_log_$dir.xlsx ]; then
        # get OSX date
        LAST_MODIFIED=$(echo -e "import datetime, os\nprint datetime.datetime.fromtimestamp(os.stat('$BASE_DIR/$dir/_ADMINISTRATION/status_log_$dir.xlsx').st_birthtime).strftime('%y_%m_%d')" | python)
        # get Linux date
        #LAST_MODIFIED=$(date '+%y-%m-%d' -r $CUR_DIR/_ADMINISTRATION/status_log_$dir.xlsx)
        mv $CUR_DIR/_ADMINISTRATION/status_log_$dir.xlsx $CUR_DIR/_ADMINISTRATION/_archived/status_log_"$dir"_"$LAST_MODIFIED".xlsx
    fi

    python $BASE_DIR/$SCRIPT_NAME --file-folder ../$CUR_DIR --output _ADMINISTRATION/status_log_$dir.xlsx --excel $BASE_DIR/$XLS_FILE

    echo "######################"
    echo "# "$dir" finished"
    echo "######################"

    cd $DEST_PATH
done
