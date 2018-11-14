#!/bin/bash

# This script will run ftArchPostProc with the -t, -a, or -n option given as
# the first command line argument. It will create exportable csv data files
# from raw sql database csv data files.

# Functions
usage()
{
    echo "usage: createExportFiles.sh -t | -a | -n"
}

# Make sure the first (and only used) argument is an expected one (-t or -a or -n)
# Create the command based on the command line argument
case $1 in
    -t | -a | -n )      ;;
    "" | -h | --help )  usage
                        exit
                        ;;
    * )                 usage
                        exit
esac

# Activate the python environment
source /home/dataadmin/swDev/python/ftArchPostProc/bin/activate

# get the path of the current script
# won't work if the script is run or sourced via a symlink
# In a seperate shell, change to the directory where the script is, and then pwd
# This is done here for convenience if it is needed later. It may or may not be
# needed
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#echo "$SCRIPT_DIR"

# Fill array with source file names without the extensions
# .csv extension is implied and assumed
# make sure the array is initially empty. Sometimes 
# unused elements can have values from an earlier run
declare -a SRC_NAMES=()

SRC_NAMES[0]="171127_00_06_Mockup"
SRC_NAMES[1]="171127_06_12_Mockup"
SRC_NAMES[2]="171127_12_18_Mockup"
SRC_NAMES[3]="171127_18_24_Mockup"
SRC_NAMES[4]="171128_00_06_Mockup"
SRC_NAMES[5]="171128_06_12_Mockup"
SRC_NAMES[6]="171128_12_18_Mockup"
SRC_NAMES[7]="171128_18_24_Mockup"
SRC_NAMES[8]="171129_00_06_Mockup"
SRC_NAMES[9]="171129_06_12_Mockup"
SRC_NAMES[10]="171129_12_18_Mockup"
SRC_NAMES[11]="171129_18_24_Mockup"
SRC_NAMES[12]="171130_00_06_Mockup"
SRC_NAMES[13]="171130_06_12_Mockup"
SRC_NAMES[14]="171130_12_18_Mockup"
SRC_NAMES[15]="171130_18_24_Mockup"
SRC_NAMES[16]="171201_00_06_Mockup"
SRC_NAMES[17]="171201_06_12_Mockup"
SRC_NAMES[18]="171201_12_18_Mockup"
SRC_NAMES[19]="171201_18_24_Mockup"
SRC_NAMES[20]="171207_00_06_Mockup"
SRC_NAMES[21]="171207_06_12_Mockup"
SRC_NAMES[22]="171207_12_18_Mockup"
SRC_NAMES[23]="171207_18_24_Mockup"
SRC_NAMES[24]="171208_00_06_Mockup"
SRC_NAMES[25]="171208_06_12_Mockup"
SRC_NAMES[26]="171208_12_18_Mockup"
SRC_NAMES[27]="171208_18_24_Mockup"
SRl_NAMES[28]="171212_00_06_Mockup"
SRC_NAMES[29]="171212_06_12_Mockup"
SRC_NAMES[30]="171212_12_18_Mockup"
SRC_NAMES[31]="171212_18_24_Mockup"

# loop thru the array of file names
# [@] denotes an indexed array
# Make sure the source file exists, if not print a message and move on
for FILE in "${SRC_NAMES[@]}"
do
    # add the extension 
    SRC_FILE=$FILE".csv" 
    # it exists or skip it
    if [ ! -f $SRC_FILE ]; then
        # The source file does not exist
        echo $SRC_FILE" does not exist. Skipping it."
    else
        # destination file with the exension
        DEST_FILE=$FILE"_ForExport.csv"

        # Create the command based on the command line argument
        # The invalid cases were caught above, so no check needed here
        case $1 in
            -t )                FTPP_CMD="ftpp -t $SRC_FILE $DEST_FILE"
                                ;;
            -a )                FTPP_CMD="ftpp -a $SRC_FILE $DEST_FILE"
                                ;;
            -n )                FTPP_CMD="ftpp -n $SRC_FILE $DEST_FILE"
                                ;;
        esac
        # echo it for informational purposes
        echo $FTPP_CMD
        # run the cmd
        eval "$FTPP_CMD"
    fi
done

# deactivate the python environment
deactivate

