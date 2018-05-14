#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Test harness for TsIdxData
# -se or --sourceEncoding (optional, default of "utf-16"). Source file encoding.
#
# -sd or --sourceDelimiter (optional, default of ","). Destination file field
# delimiter. Single character or regex.
#
# -dd or --destDelimiter (optional, default of ","). Destination file field
# delimiter. Single character or regex.
#
# -de or --destEncoding (optional, default of "utf-16"). Destination file encoding.
#
# -vq or --valueQuery (optional, default=None). Query string used to filter
# the dataset. Default is empty, so nothing is filtered out. Use "val" to
# represent the process value(s). e "==" for an equlity test.  For example, 
# to filter out all values < 0 or > 100, you want to keep everything else,
# so the filter string would be:
#    "val >= 0 and val <= 100".
#
# -st or --startTime (optional, default=None)
# Specify a start date and time. If a time and no date is specified, the
# current date is used.  If a date and no time is specified, midnight is
# used so the entire date is included.  If this argument is not used, the 
# start time is derived from the data, and the earliest of all the data
# timestamps is used.

# -et or --endTime (optional, default=None)
# Specify an end date and time. If a time and no date is specified, the
# current date is used.  If a date and no time is specified, the moment before
# midnight (11:59:59.999) is used so the  entire date is included.  If this
# argument is not used, the end time is derived from the data, and the latest
# of all the data timestamps is used.
#
# -stf or --sourceTimeFormat (optional, default="%m/%d/%Y %I:%M:%S %p")
# Specify the format of the source data time format,
# as a string. Use the following placeholders: %m minutes, %d days, %Y 4 digit
# year, %y two digit year, %H hours (24hr format) %I hours (12 hr format), %M
# minutes, %S seconds, %f for fractional seconds (e.g. %S.%f), %p AM/PM.
# The default string is "%m/%d/%Y %H:%M:%S.%f".'
#
# -rs or --resample (optional, default=None) Resample the data. This is usually
# used to "downsample" data. For example, create an output file with 1 sample
# per minute when given an input file with 1 sample per second. If a period
# longer than the source data sample period is specified, then one value is
# used to represent more than one row in the source file.  In this case, the
# -stats option is used (see below) to specify what statistices are calculated
# for the rolled up values. 
# Options are (D)ay, (H)our, minu(T)e, (S)econd, mi(L)liseconds, and are not
# case sensitive. You can put an integer in front of the option to further
# specify a period. For example, "5S" would be a 5 second sample period. Note
# that other options are supported by the environment, but unexpected sample
# times may result.
#
# -stats (optional, default='m') Choose which statistics to calculate when
# resampling. Ignored if not resampling (-rs must be specified for this option
# to do anything).  Choices are: (V)alue, m(I)n, ma(X), (a)verage/(m)ean,
# and (s)tandard deviation. Choices are not case sensitive. Default is 
# average/mean.  In the case of the Value option, the first value available
# which is on or after the timestamp is shown. The values between this and the
# next sample point are thrown away. For the other options, the intermediate
# values are used to calculate the statistic.
#
# -noExportMsg (optional, default=False). When this argument is used, it turns
# off the inclusion of an export control message.  The defaults to false, so a
# message is included unless this argument is specified.
# 
# imports
#
# system related
import sys
# date and time stuff
from datetime import datetime, time
from pandas.tseries.frequencies import to_offset
from dateutil import parser as duparser

# csv file stuff
import csv

# arg parser
import argparse

# numerical manipulation libraries
import numpy as np
import pandas as pd

# custom libraries
from TsIdxData import TsIdxData


# **** argument parsing
# define the arguments
# create an epilog string to further describe the input file
eplStr="""TsIdxData Test Harness
 This program accepts an input csv file, post processes it, and creates a csv
 output file.  An export control message is included at the head of the output
 file, unless the -noExportMsg argument is used.

 Field delimiters can be specified for the input and output files. The
 default field delimiter is the comma (","). If another delimiter needs to
 be specified, it can be done so using the -sd, -sourceDelimiter, -dd, or
 -destDelimiter options. If more than one character is specified, the
 delimiter will be interpreted as a regular expression.

 File encoding can be specified for the input and output files. The default
 encoding is "utf-16". If another encoding needs to be specified, it can be
 done using the -se, -sourceEncoding, -de, or -destEncoding options.


 Command line arguments are:
 inputFileName (required, positional). The source data csv file.

 outputFileName (required, positional). The .csv output file name.

 -se or --sourceEncoding (optional, default of "utf-16"). Source file encoding.

 -sd or --sourceDelimiter (optional, default of ","). Destination file field
 delimiter. Single character or regex.

 -dd or --destDelimiter (optional, default of ","). Destination file field
 delimiter. Single character or regex.

 -de or --destEncoding (optional, default of "utf-16"). Destination file encoding.

 -vq or --valueQuery (optional, default=None). Query string used to filter
 the dataset. Default is empty, so nothing is filtered out. Use "val" to
 represent the process value(s). Use "==" to test for equality. 
 For example, to filter out all values < 0 or > 100, you want to keep everything
 else, so the filter string would be:
    "val >= 0 and val <= 100".

 -st or --startTime (optional, default=None)
 Specify a start date and time. If a time and no date is specified, the
 current date is used.  If a date and no time is specified, midnight is
 used so the entire date is included.  If this argument is not used, the 
 start time is derived from the data, and the earliest of all the data
 timestamps is used.

 -et or --endTime (optional, default=None)
 Specify an end date and time. If a time and no date is specified, the
 current date is used.  If a date and no time is specified, the moment before
 midnight (11:59:59.999) is used so the  entire date is included.  If this
 argument is not used, the end time is derived from the data, and the latest
 of all the data timestamps is used.

 -stf or --sourceTimeFormat (optional, default="%m/%d/%Y %I:%M:%S %p")
 Specify the format of the source data time format,
 as a string. Use the following placeholders: %m minutes, %d days, %Y 4 digit
 year, %y two digit year, %H hours (24hr format) %I hours (12 hr format), %M
 minutes, %S seconds, %f for fractional seconds (e.g. %S.%f), %p AM/PM.
 The default string is "%m/%d/%Y %H:%M:%S.%f".

 -rs or --resample (optional, default=None) Resample the data. This is usually
 used to "downsample" data. For example, create an output file with 1 sample
 per minute when given an input file with 1 sample per second. If a period
 longer than the source data sample period is specified, then one value is
 used to represent more than one row in the source file.  In this case, the
 -stats option is used (see below) to specify what statistices are calculated
 for the rolled up values. 
 Options are (D)ay, (H)our, minu(T)e, (S)econd, mi(L)liseconds, and are not
 case sensitive. You can put an integer in front of the option to further
 specify a period. For example, "5S" would be a 5 second sample period. Note
 that other options are supported by the environment, but unexpected sample
 times may result.

 -stats' (optional, default='m') Choose which statistics to calculate when
 resampling. Ignored if not resampling (-rs must be specified for this option
 to do anything).  Choices are: (V)alue, m(I)n, ma(X), (a)verage/(m)ean,
 and (s)tandard (d)eviation (Note: Std Dev is "s" OR "d", not both.)
 Choices are not case sensitive. Default is average/mean.
 In the case of the Value option, the first value available
 which is on or after the timestamp is shown. The values between this and the
 next sample point are thrown away. For the other options, the intermediate
 values are used to calculate the statistic.

 -noExportMsg (optional, default=False). When this argument is used, it turns
 off the inclusion of an export control message.  The defaults to false, so a
 message is included unless this argument is specified. """

descrStr="Post Processing of historical trend or archive data files."
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
                                 description=descrStr, epilog=eplStr)
parser.add_argument('inputFileName', help='Input data file (csv)')
parser.add_argument('outputFileName', help= 'Output data file (csv)')
parser.add_argument('-sd', '--sourceDelimiter', default=',', metavar='', \
                   help='Source file field delimiter. Default is a comma (\",\").')
parser.add_argument('-se', '--sourceEncoding', default='utf_16', metavar='', \
                   help='Source file encoding. Default is utf_16.')
parser.add_argument('-dd', '--destDelimiter', default=',', metavar='', \
                   help='Destination file field delimiter. Default is a comma (\",\").')
parser.add_argument('-de', '--destEncoding', default='utf_16', metavar='', \
                   help='Source file encoding. Default is utf_16.')
parser.add_argument('-vq', '--valueQuery', default=None, metavar='', \
                   help='Query string used to filter the dataset. \
Default is empty, so nothing is filtered out. Use "val" to represent the \
process value(s). For example, to filter out all values < 0 or > 100,\
you want to keep everything else, so the filter string would be: \
"val >= 0 and val <= 100".')
parser.add_argument('-st', '--startTime', default=None, metavar='', \
                    help='Specify a start date and time. If a time and no \
date is specified, the current date is used.  If a date and no time \
is specified, midnight is used so the entire date is included.  If this \
argument is not used, the start time is derived from the data, and the \
earliest of all the data timestamps is used.')
parser.add_argument('-et', '--endTime', default=None, metavar='', \
                    help='Specify an end date and time. If a time and no \
date is specified, the current date is used.  If a date and no time \
is specified, the moment before midnight (11:59:59.999) is used so the \
entire date is included.  If this argument is not used, the end time is \
derived from the data, and the latest of all the data timestamps is used.')
parser.add_argument('-stf', '--sourceTimeFormat', \
                    default='%m/%d/%Y %H:%M:%S.%f', metavar='', \
                    help='Specify the format of the source data time format, \
as a string. Use the following placeholders:%%m minutes, %%d days, %%Y 4 digit \
year, %%y two digit year, %%H hours (24hr format) %%I hours (12 hr format), %%M \
minutes, %%S seconds, %%f for fractional seconds (e.g. %%S.%%f), %%p AM/PM. \
The default string is "%%m/%%d/%%Y %%H:%%M:%%S.%%f".')
parser.add_argument('-rs', '--resample', default=None, metavar='', \
                    help='Resample the data. This is usually \
 used to "downsample" data. For example, create an output file with 1 sample \
 per minute when given an input file with 1 sample per second. If a period \
 longer than the source data sample period is specified, then one value is \
 used to represent more than one row in the source file.  In this case, the \
 -stats option is used (see below) to specify what statistics are calculated \
 for the rolled up values. \
 Options are (D)ay, (H)our, minu(T)e, (S)econd, mi(L)liseconds, and are not \
 case sensitive. You can put an integer in front of the option to further \
 specify a period. For example, "5S" would be a 5 second sample period. Note \
 that other options are supported by the environment, but unexpected sample \
 times may result.')
parser.add_argument('-stats', default='m', metavar='', \
                    help='Choose which statistics to calculate when \
 resampling. Ignored if not resampling (-rs must be specified for this option \
 to do anything).  Choices are: (V)alue, m(I)n, ma(X), (a)verage/(m)ean, \
 and (s)tandard (d)eviation (Note: Std Dev is "s" OR "d", not both.) \
 Choices are not case sensitive. Default is average/mean. \
 In the case of the Value option, the first value available \
 which is on or after the timestamp is shown. The values between this and the \
 next sample point are thrown away. For the other options, the intermediate \
 values are used to calculate the statistic.')

parser.add_argument('-noExportMsg', action='store_true', default=False, \
                    help='Do not include the export control message at the \
head of the output file when specified.')

# parse the arguments
args = parser.parse_args()

# At this point, the arguments will be:
# Argument          Values      Description
# args.inputFileName    string  file to get data from
# args.outputFileName   string  file to write processed data to
# args.sourceDelimiter  string  Input file field delimiter. Default is ","
# args.sourceEncoding   string  Input file encoding. Default is utf_16.
# args.destDelimiter    string  Dest file field delimiter. Default is (",")
# args.destEncoding     string  Dest file encoding. Default is utf_16.
# args.valueQuery       string  Optional query of the data
# args.startTime        string  Optional start date time
# args.endTime          string  Options end date time
# args.sourceTimeFormat string  Format string for source data timestamps
# args.resample         string  Resample period. Default is 'S' or 1 sample/sec.
# args.stats            string  Stats to calc. Value, min, max, ave, std dev.
# args.noExportMsg      True/False Exclude export control message when set

# Put the begin mark here, after the arg parsing, so argument problems are
# reported first.
print('**** Begin Processing ****')
# get start processing time
procStart = datetime.now()
print('    Process start time: ' + procStart.strftime('%m/%d/%Y %H:%M:%S'))

# **** Convert the start and end times to datetimes if they are specified.
# Use the dateutil.parser function to get input flexability, and then
# convert to a pandas datetime for max compatibility
# If time info is not included in the start time, it defaults to
# midnight, so comparisons will work as expected and capture the entire
# day. For the end time, however, if time info is not included, force
# it to be 11:59:59.999 so the entire end date is captured.
if args.startTime is not None:
    # Convert the argument to a datetime. If it can't be converted, ignore it.
    # need to convert
    try:
        startArg = duparser.parse(args.startTime, fuzzy=True)
        # convert to a pandas datetime for max compatibility
        startArg = pd.to_datetime(startArg, errors='raise', box=True,
                                  infer_datetime_format=True, origin='unix')
    except:
        # not convertable ... invalid ... ignore
        print('WARNING: Invalid start time. Ignoring.')
        startArg = None
else:
    # arg is none, so update the internal version
    startArg = None

# repeat for end time
if args.endTime is not None:
    # Convert the argument to a datetime. If it can't be converted, ignore it.
    try:
        endArg = duparser.parse(args.endTime, fuzzy=True)
        # convert to a pandas datetime for max compatibility
        endArg = pd.to_datetime(endArg, errors='raise', box=True,
                                  infer_datetime_format=True, origin='unix')

        # assume the end time of midnight means end time info was not
        # specified. Force it to the end of the day
        if endArg.time() == time(0,0,0,0):
            endArg = endArg.replace(hour=23, minute=59, 
                                    second=59, microsecond=999999)

    except:
        # not convertable ... invalid ... ignore
        print('WARNING: Invalid end time. Ignoring.')
        endArg = None
    
else:
    # arg is none, so update the internal version
    endArg = None

# get the resample argument
if args.resample is not None:
    # a resample arg was supplied.  Try to use it, or default to 1 sec.
    try:
        resampleArg = to_offset(args.resample) # use the offset version
    except:
        print('WARNING: Invalid resample period specified. Using 1 second')
        resampleArg = to_offset('S')
else:
    # arg is none, so update the internal version
    resampleArg = None

# force the stats argument to a lower case string so they are case insensitive.
stats = str(args.stats).lower()


# make sure the source timestamp format argument is a string
sourceTimeFormat = str(args.sourceTimeFormat)

# **** read the csv file into a data frame.  The first row is treated as the header
try:
    # use string as the data type for all columns to prevent automatic
    # datatype detection. We don't know ahead of time how many columns are
    # being read in, so we don't yet know the types.
    df_source = pd.read_csv(args.inputFileName, sep=args.sourceDelimiter,
                        delim_whitespace=False, encoding=args.sourceEncoding,
                        header=0, dtype = str, skipinitialspace=True)
except:
    print('ERROR opening source file: "' + args.inputFileName + '". Check file \
name, file presence, and permissions. Unexpected encoding can also cause this \
error.')
    quit()

print('**** Input Data ****')
print(df_source)

# put source the headers into a list
headerList = df_source.columns.values.tolist()
# Make a spot for a list of instrument TsIdxData objects,
# and a list of just instrument names. The latter is used to detect
# data for duplicate instruments. If a duplicate is found, the data is
# appended to an already existing instrument.
instData = []
tid = TsIdxData('TT6Jimmy', 'TT6JimmyTime', 'TT6ValueY', df_source,
                args.valueQuery, startArg, endArg,  sourceTimeFormat)
print('\n**** Our test TsIdxData is ... the big reveal!! ****')
print(tid)


#get end  processing time
procEnd = datetime.now()
print('\n**** End Processing ****')
print('    Process end time: ' + procEnd.strftime('%m/%d/%Y %H:%M:%S'))
print('    Duration: ' + str(procEnd - procStart) + '\n')

quit()
