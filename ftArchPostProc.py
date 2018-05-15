#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Final Test Archive Data Post Processing
# This program accepts an input csv file, post processes it, and creates a csv
# output file.  An export control message is included at the head of the output
# file, unless the -noExportMsg argument is used.
#
# In the case of a historical trend generated file (the -t command line
# argument), the data columns are as follows:
#   Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
# and the timestamps are not synchronized.
#
# In the case of a archive export file (the -a command line argument), the data
# columns are as follows:
#   TagId,TagName,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),DataSource,Value,Quality
# Where normally there are multiple tags each at multiple repeating
# timestamps. Timestamps are not necessarily synchronized.
#
# In the case of a time normalized export file (the -n command line argument),
# the data columns are as follows:
#   Timestamp, Time Bias, Tag1 Value, Tag2 Value, Tag3 Value ...
# where Timestamp is in local time, and the Bias is a time zone adjustment value,
# in minutes, that gets added to the Timestamp to get UTC.
#
# Note: The -h, -n, and -a options are mutually exclusive. One and only one must
# be specified.
#
# TODO: Update merge related params
#
# Given an input file, the program will produce a *.csv file with the name
# specified as the outputFileName with the format:
# Timestamp, Tag1 Value, Tag2 Value ...
#
# Field delimiters can be specified for the input and output files. The
# default field delimiter is the comma (","). If another delimiter needs to
# be specified, it can be done so using the -sd, -sourceDelimiter, -dd, or
# -destDelimiter options. If more than one character is specified, the
# delimiter will be interpreted as a regular expression.
#
# File encoding can be specified for the input and output files. The default
# encoding is "utf-16". If another encoding needs to be specified, it can be
# done using the -se, -sourceEncoding, -de, or -destEncoding options.
#
# It is assumed that the first row is a header. In the case of a historical
# trend input file (-t option), the tag names are derrived from the header.
# In the case of a archive data input file (-a option), the tag names are 
# pulled from the data.
#
# Command line arguments are:
# inputFileName (required, positional). The source data csv file.
#
# outputFileName (required, positional). The .csv output file name.
#
# -t, (required and mutually exclusive with -a and -n).  Input file
# is a historical trend export file.  The format is:
#     Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
#
# -a (required and mutually exclusive with -t and -n). Input file is a
# archive export file. The format is:
#     TagId,TagName,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),DataSource,Value,Quality
#
# -n (required and mutually exclusive with -t and -a). Input file is a time
# normalized export file.  The format is:
#     Timestamp, Time Bias, Tag1 Value, Tag2 Value, Tag3 Value ...
#
# -am1, -am2, -am3, -am4 or --archiveMergen (optional, default=None). Archive Merge.
# Merge these named files with the data in the inputFileName before processing.
# Must have the same format/layout as the input file.
#
# -mf or --mergeFill (optional, default='ffill') Merge fill. How to fill missing data
# when merging files historical trend files (-t option).
# Options are "ffill" or "pad" to fill from last valid value to the next valid value,
# "backfill" or "bfill" to fill using the next valid value to back fill a gap, 
# "value" to use a value specified with the -mv option, or "none" to use
# np.nan (not a number representation). Ignored if merge files are not specified.
#
# -mv or --mergeValue (optional, float, default=-9999.0). Used when -mf option
# is specified as "value".  Ignored if merge files are not specified or if -mf is
# not "value"
#
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
# represent the process value(s). For example, to filter out all
# values < 0 or > 100,you want to keep everything else, so the filter string
# would be:
# "val >= 0 and val <= 100".
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
# The default string is "%m/%d/%Y %I:%M:%S %p".'
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
# TODO: Improved Error handling? Testing will tell if this is needed.
#
# TODO: Include units. This does not come from the data export. One idea is to
# use a JSON file to map tag name with units.  Additionally, a JSON file may be
# used in the archive data (-a option) file to map tag name with ID number. If 
# this is the case, then the same JSON file could be used by both the -t and
# -a options.
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
# not necessarily synchronized.

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
eplStr="""Final Test Archive Data Post Processing
 This program accepts an input csv file, post processes it, and creates a csv
 output file.  An export control message is included at the head of the output
 file, unless the -noExportMsg argument is used.

 In the case of a historical trend generated file (the -t command line
 argument), the data columns are as follows:
     Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
 and the timestamps are not synchronized.

 In the case of a archive export file (the -a command line argument), the data
 columns are as follows:
     TagId,TagName,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),DataSource,Value,Quality
 Where normally there are multiple tags each at multiple repeating
 timestamps. Timestamps are not necessarily synchronized.

 In the case of a time normalized export file (the -n command line argument), the data
 columns are as follows:
     Timestamp, Time Bias, Tag1 Value, Tag2 Value, Tag3 Value ...
 where Timestamp is in local time, and the Bias is a time zone adjustment value,
 in minutes, that gets added to the Timestamp to get UTC.

 Note: The -h, -n, and -a options are mutually exclusive. One and only one must
 be specified.

 Given an input file, the program will produce a *.csv file with the name
 specified as the outputFileName with the format:
 Timestamp, Tag1 Value, Tag2 Value ...

 Field delimiters can be specified for the input and output files. The
 default field delimiter is the comma (","). If another delimiter needs to
 be specified, it can be done so using the -sd, -sourceDelimiter, -dd, or
 -destDelimiter options. If more than one character is specified, the
 delimiter will be interpreted as a regular expression.

 File encoding can be specified for the input and output files. The default
 encoding is "utf-16". If another encoding needs to be specified, it can be
 done using the -se, -sourceEncoding, -de, or -destEncoding options.

 It is assumed that the first row is a header. In the case of a historical
 trend input file (-t option), the tag names are derrived from the header.
 In the case of a archive data input file (-a option), the tag names are 
 pulled from the data.
 

 Command line arguments are:
 inputFileName (required, positional). The source data csv file.

 outputFileName (required, positional). The .csv output file name.

 -t, (required and mutually exclusive with -a and -n).  Input file
 is a historical trend export file.  The format is:
     Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...

 -a (required and mutually exclusive with -t and -n). Input file is a
 archive export file. The format is:
     TagId,TagName,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),DataSource,Value,Quality

 -n (required and mutually exclusive with -t and -a). Input file is a time
 normalized export file.  The format is:
     Timestamp, Time Bias, Tag1 Value, Tag2 Value, Tag3 Value ...

 -am1, -am2, -am3, -am4 or --archiveMergen (optional, default=None). Archive Merge.
 Merge these named files with the data in the inputFileName before processing.
 Must have the same format/layout as the input file.

 -mf or --mergeFill (optional, default='ffill') Merge fill. How to fill missing data
 when merging files historical trend files (-t option).
 Options are "ffill" or "pad" to fill from last valid value to the next valid value,
 "backfill" or "bfill" to fill using the next valid value to back fill a gap, 
 "value" to use a value specified with the -mv option, or "none" to use
 np.nan (not a number representation). Ignored if merge files are not specified.

 -mv or --mergeValue (optional, float, default=-9999.0). Used when -mf option
 is specified as "value".  Ignored if merge files are not specified or if -mf is
 not "value"

 -se or --sourceEncoding (optional, default of "utf-16"). Source file encoding.

 -sd or --sourceDelimiter (optional, default of ","). Destination file field
 delimiter. Single character or regex.

 -dd or --destDelimiter (optional, default of ","). Destination file field
 delimiter. Single character or regex.

 -de or --destEncoding (optional, default of "utf-16"). Destination file encoding.

 -vq or --valueQuery (optional, default=None). Query string used to filter
 the dataset. Default is empty, so nothing is filtered out. Use "val" to
 represent the process value(s). For example, to filter out all
 values < 0 or > 100,you want to keep everything else, so the filter string
 would be: "val >= 0 and val <= 100".

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
 The default string is "%m/%d/%Y %I:%M:%S %p".

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
parser.add_argument('-am1', '--archiveMerge1', default=None, metavar='', \
                   help='Merge this named file with the data in the \
inputFileName before processing. Must be used with the -a option. \
Must have the same format/layout as the input file.')
parser.add_argument('-am2', '--archiveMerge2', default=None, metavar='', \
                   help='Merge this named file with the data in the \
inputFileName before processing. Must be used with the -a option. \
Must have the same format/layout as the input file.')
parser.add_argument('-am3', '--archiveMerge3', default=None, metavar='', \
                   help='Merge this named file with the data in the \
inputFileName before processing. Must be used with the -a option. \
Must have the same format/layout as the input file.')
parser.add_argument('-am4', '--archiveMerge4', default=None, metavar='', \
                   help='Merge this named file with the data in the \
inputFileName before processing. Must be used with the -a option. \
Must have the same format/layout as the input file.')
parser.add_argument('-mf', '--mergeFill', default='ffill', \
                    choices=['ffill', 'pad', 'bfill', 'backfill', 'value', 'none'], metavar='', \
                    help='Merge fill. How to fill missing data when merging files. \
Options are "ffill" or "pad" to fill from last valid value \
to the next valid value, "backfill" or "bfill" to fill using the next valid \
value to back fill a gap, "value" to use a value specified with the -mv option, \
or "none" to use np.nan (not a number representation). \
Ignored if merge files are not specified.')
parser.add_argument('-mv', '--mergeValue', type=float, default=9999.0, metavar='', \
                    help='Used when -mf option is specified as "value". \
Ignored if merge files are not specified or if -mf is not "value"')
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
                    default='%m/%d/%Y %I:%M:%S %p', metavar='', \
                    help='Specify the format of the source data time format, \
as a string. Use the following placeholders:%%m minutes, %%d days, %%Y 4 digit \
year, %%y two digit year, %%H hours (24hr format) %%I hours (12 hr format), %%M \
minutes, %%S seconds, %%f for fractional seconds (e.g. %%S.%%f), %%p AM/PM. \
The default string is "%%m/%%d/%%Y %%I:%%M:%%S %%p".')
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

# add -t and -a as a required, but mutually exclusive group
typegroup = parser.add_mutually_exclusive_group(required=True)
typegroup.add_argument('-t',  action='store_true', default=False, \
                    help='Historical trend input file type (format).')
typegroup.add_argument('-a', action='store_true', default=False, \
                    help='Archive data input file type (format).')
typegroup.add_argument('-n', action='store_true', default=False, \
                    help='Time normalized input file type (format).')
# parse the arguments
args = parser.parse_args()

# At this point, the arguments will be:
# Argument          Values      Description
# args.inputFileName    string  file to get data from
# args.outputFileName   string  file to write processed data to
# args.archiveMerge1    string  file to merge with input
# args.archiveMerge2    string  file to merge with input
# args.archiveMerge2    string  file to merge with input
# args.archiveMerge4    string  file to merge with input
# args.mergeFill        string  method to fill voids in merged file.
#                               Default is 'ffill'
# args.mergeValue       float   value to use for voids in merged file if mergeFill is "value".
#                               Default is 9999.0
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
# args.t                True/False  Historical trend input file type when set
# args.a                True/False  Archive data input file type when set
# args.n                True/False  Time Normalized input file type when set.

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
instDataNames = []


# **** Look at the data type being input (-t, -a, or -n) and make sure there
# is at least the minimum number of columns for a valid data file. If there are
# any merge files specified (-am params), then merge them. Finally process the 
# data into a list of TsIdxData objects, with one object per instrument holding 
# the time stamped indexed value data.
if args.t and len(headerList) >= 2:
    # Historical trend data, and there are at least two (time/value pair) cols.
    # The data is expected to have these columns
    # [0] Tag 1 Timestamp
    # [1] Tag 1 Value
    # ...
    # [(n-1) * 2] Tag n Timestamp
    # [((n-1) * 2) + 1] Tag n Value
    # where the header contains the instrument names plus a suffix for the
    # timestamp and value columns.
    #
    # In the historical trend case, loop thru every other column to get to the 
    # time stamp columns. The instrument name can be derived from this and the 
    # values can be obtained from a relative (+1) index from the timestamp
    print('\nHistorical Trend Data Specified. The source data is expected to \
have the following format:\n \
    Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2 Value ... \n\
and the timestamps may or may not be synchronized.')

    # If there are files specified to merge, merge them with the input file before 
    # further processing. Since this file format has independent time/value pairs
    # in columns going to the right, this merge simply makes the source data wider
    # by appending columns (pd.concat with axis=1).
    # Merge File 1
    if args.archiveMerge1 is not None:
        try:
            print('Merging file "' + args.archiveMerge1 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge1, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
            #
        except:
            print('ERROR opening the file specified with the -am1/archiveMerge1 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append columns (axis = 1), keeping the header rows.
        # There may be NaN values present when/if columns are different length. 
        # This isn't different than in the input file.
        df_merged = pd.concat([df_source, df_merge], axis=1, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)

        del df_source
        del df_merge
        df_source = df_merged
        del df_merged

# Merge File 2
    if args.archiveMerge2 is not None:
        try:
            print('Merging file "' + args.archiveMerge2 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge2, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am2/archiveMerge2\
parameter: "' + args.archiveMerge2 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append columns (axis = 1), keeping the header rows.
        # There may be NaN values present when/if columns are different length. 
        # This isn't different than in the input file.
        df_merged = pd.concat([df_source, df_merge], axis=1, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)

        del df_source
        del df_merge
        df_source = df_merged
        del df_merged

    # Merge File 3
    if args.archiveMerge3 is not None:
        try:
            print('Merging file "' + args.archiveMerge3 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge3, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am3/archiveMerge3 \
parameter: "' + args.archiveMerge3 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append columns (axis = 1), keeping the header rows.
        # There may be NaN values present when/if columns are different length. 
        # This isn't different than in the input file.
        df_merged = pd.concat([df_source, df_merge], axis=1, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)

        del df_source
        del df_merge
        df_source = df_merged
        del df_merged
        
    # Merge File 4
    if args.archiveMerge4 is not None:
        try:
            print('Merging file "' + args.archiveMerge4 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge4, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am4/archiveMerge4 \
parameter: "' + args.archiveMerge1+ '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append columns (axis = 1), keeping the header rows.
        # There may be NaN values present when/if columns are different length. 
        # This isn't different than in the input file.
        df_merged = pd.concat([df_source, df_merge], axis=1, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)

        del df_source
        del df_merge
        df_source = df_merged
        del df_merged
        
    # update the header list after the merge to make sure new tags are reflected.
    headerList = df_source.columns.values.tolist()
    print('**** Header List ****')
    print(headerList)
    # sort the data by time
    df_source.sort_index(inplace=True)
    
    # TODO: Make sure duplicate rows are handled.
    # Needed here or already taken care of below?

    # Loop thru the header list. Get the instrument name, create a data frame for
    # each instrument, get the timestamp to be a datetime and the value to be a
    # float, create a list of instruments and an list of TsIdxData objects (one
    # per instrument).  If the instrument name is duplicated, the data sets are
    # merged.
    for idx in range(0, len(headerList), 2):
        # For each header entry, make instrument and timestamp column names.
        # Even indexes are timestamps, odd indexes are values.
        # Get the inst name, leaving off the bit after the last space, which is
        # normally 'Time' or 'ValueY'
        # rpartition returns a tuple: first, separator, last. Use the first 
        # member as the tag name -- this allows tag names with spaces to be
        # preserved
        instName = headerList[idx].rpartition(' ')[0] 
        # replace the spaces, hyphens, and periods with underscores
        instName = instName.replace(' ', '_')
        instName = instName.replace('-', '_')
        instName = instName.replace('.', '_')
        # Generate timestamp and value field (column) names.
        # These will be used for the exported data.
        # Include the instr name in the timestamp column label so it can be
        # identified standalone
        tsName = 'timestamp_' + instName
        valName = 'value_' + instName
        # print a message showing what we are processing
        print('\nProcessing ' + instName)
        # create a new instrument object 
        # create a new dataframe for the instrument and use the above column names
        df_valData = pd.DataFrame(data=df_source.iloc[:,[idx,idx+1]])
        df_valData.columns = [tsName, valName]
        print('**** df_valData ****')
        print(df_valData)
        # create a new instrument object 
        tid_inst = TsIdxData(instName, tsName, valName, df_valData, 
                             args.valueQuery, startArg, endArg, sourceTimeFormat)
        print('****tid_inst ****')
        print(tid_inst)
        quit()
        # change the data types of the timestamp and value columns if needed
        # value data needs to be float
        if 'float64' != df_valData[valName].dtype:
            # not a float, but it should be. Change the type
            df_valData[valName] = df_valData[valName].astype('float',errors='ignore')
        # timestamp needs to be a date time
        if 'datetime64[ns]' != df_valData[tsName].dtype:
            # For changing to timestamps, coerce option for errors is marking
            # dates after midnight (next day) as NaT.
            # Not sure why. Try it with raise, first, and you get
            # all the values. Put it in a try block, just in case an error is
            # raised.
            try:
                df_valData[tsName] = pd.to_datetime(df_valData[tsName],
                                                errors='raise',
                                                box = True, 
                                                format=sourceTimeFormat,
                                                exact=False,
                                                #infer_datetime_format = True,
                                                origin = 'unix')
            except:
                print('    WARNING: Problem converting some timestamps from \
the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                df_valData[tsName] = pd.to_datetime(df_valData[tsName],
                                                errors='coerce',
                                                box = True, 
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # set the timestamp column to be the index
        df_valData.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_valData.sort_index(inplace=True)
        
        # See if instrument is already in the list. If so append the 
        # data to an existing instrument object already in the object list.
        # If not, then append a new object with the new data to the name and
        # object lists.
        print('**** InstDataNames before check for "' + instName + '" ****')
        print(instDataNames)
        if instName in instDataNames:
            # An instrument with the same name already exists. 
            # Append this data to it
            idx = instDataNames.index(instName)
            print('Inst in list at index ' + str(idx) + '. Appending data.')

            # Appending the data will apply previously specified value queries
            # and time filtering
            instData[idx].appendData(df_valData, 0) # don't ignore any rows
            print(instData[idx])
        else:
            # This instrument is not in the instrument list yet.  
            # Append it to the name list and the object list
            print('Inst not yet seen. Appending new instrument to list of instruments.')
            instDataNames.append(instName)
            # Make an object with the instrument name, labels and data frame
            # instrument data object, and append it to the list.
            # Querying of value and filtering of timestamps will happen during
            # construction of the object
            instData.append(TsIdxData(instName, tsName, valName, df_valData,
                                     args.valueQuery, startArg, endArg,
                                     sourceTimeFormat))

        # The instrument data is now contained in the instrument InstData object.
        # Delete the valData dataframe to free up resources.
        del df_valData

    # The data is now in instData in data frames. Done with the source data. Delete it.
    del df_source

elif args.a and len(headerList) >= 6:
    # archive data, and at least the expected number of columns are present
    # The data is expected to these columns:
    #     [0] TagId
    #     [1] TagName
    #     [2] Timestamp
    #     [3] DataSource
    #     [4] Value
    #     [5] Quality
    print('\nArchive data file specified. The data is expected to be formatted as \
follows:\n    TagId, TagName, Timestamp (YYYY-MM-DD HH:MM:SS.mmm), DataSource, Value, Quality\n \
Where normally there are multiple tags each at multiple timestamps. Timestamps are \
not necessarily synchronized.\n')

    # If t,nhere are files specified to merge, merge them with the input file before 
    # further processing. Since the file format has rows being unique on 
    # TagId and Timestamp combo, this merge simply makes the source data longer
    # by appending rows (pd.concat with axis=0).
    # Merge File 1
    if args.archiveMerge1 is not None:
        try:
            print('Merging file "' + args.archiveMerge1 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge1, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am1/archiveMerge1 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append rows (axis = 0), ignoring overlapping 
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0, ignore_index=True, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Input Data ****')
        print(df_source)
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged

    # Merge File 2
    if args.archiveMerge2 is not None:
        try:
            print('Merging file "' + args.archiveMerge2 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge2, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am2/archiveMerge2 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append rows (axis = 0), ignoring overlapping 
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0, ignore_index=True, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Input Data ****')
        print(df_source)
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged
    
    # Merge File 3
    if args.archiveMerge3 is not None:
        try:
            print('Merging file "' + args.archiveMerge3 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge3, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am3/archiveMerge3 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append rows (axis = 0), ignoring overlapping 
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0, ignore_index=True, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Input Data ****')
        print(df_source)
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged

    # Merge File 4
    if args.archiveMerge4 is not None:
        try:
            print('Merging file "' + args.archiveMerge4 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge4, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am4/archiveMerge4 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Now merge the data. Append rows (axis = 0), ignoring overlapping 
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0, ignore_index=True, join='outer')
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        print('**** Input Data ****')
        print(df_source)
        print('**** Merge Data ****')
        print(df_merge)
        print('**** Merged Data ****')
        print(df_merged)
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged
    
    # From the source data, create a data frame with just the
    # tag id, tag name, time stamp, value
    # where the tag id and time stamp is a multi-index
    df_valData = df_source.drop(columns=[headerList[3], headerList[5]],
                                inplace=False, errors='ignore')
    # So sorting works as expected, before setting the indexes,
    # set the tag id to an int, the value to a float, and the timestamp to a datetime
    df_valData[headerList[0]] = df_valData[headerList[0]].astype('int',errors='ignore')
    df_valData[headerList[4]] = df_valData[headerList[4]].astype('float',errors='ignore')
    # force the timestamp to be a datetime
    # coerce option for errors is marking dates after midnight (next
    # day) as NaT. Not sure why. Try it with raise, first, and you get
    # all the values. Put it in a try block, just in case an error is
    # raised.
    try:
        df_valData[headerList[2]] = pd.to_datetime(df_valData[headerList[2]],
                                                   errors='raise',
                                                   box = True, 
                                                   format=sourceTimeFormat,
                                                   exact=False,
                                                   #infer_datetime_format = True,
                                                   origin = 'unix')
    except:
        print('    WARNING: Problem converting some timestamps from \
the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
        df_valData[headerList[2]] = pd.to_datetime(df_valData[headerList[2]],
                                                   errors='coerce',
                                                   box = True, 
                                                   infer_datetime_format = True,
                                                   origin = 'unix')

    # now set the index to a multi-index of TagId,Timestamp
    df_valData.set_index([headerList[0],headerList[2]], inplace=True)
    # sort the index for possible better performance later
    df_valData.sort_index(inplace=True)

    # Now create a dataframe to hold a unique list of tag ids and tag names.
    df_tagList = df_source.drop(columns=[headerList[2],headerList[3],
                                         headerList[4],headerList[5]],
                                inplace=False, errors='ignore')
    # force the id column datatype to an int
    df_tagList[headerList[0]] = df_tagList[headerList[0]].astype('int',errors='ignore')
    # Drop the duplicate ids
    df_tagList.drop_duplicates(subset=headerList[0], keep='first', inplace=True)
    # Set the index to the tagId
    df_tagList.set_index(headerList[0], inplace=True)
    # sort the index for possible better performance later
    df_tagList.sort_index(inplace=True)

    # done with the source data. Delete it.
    del df_source

    # Now we have a sorted list of tags and a dataframe full of values.
    # Go thru the tag list and make an intrument for each tag with the data.
    # The TsIdxData object is used for each instruments data.
    for trow in df_tagList.itertuples(index=True, name=None):
        # itertuples will return a tuple (id, tagname) for a row
        # For tag, get the id and the name.
        # rpartition returns a tuple: first, separator, last. Use the first 
        # member as the tag name -- this allows tag names with spaces to be
        # preserved
        instId = trow[0]
        instName = trow[1]
        # replace the spaces, hyphens, and periods with underscores
        instName = instName.replace(' ', '_')
        instName = instName.replace('-', '_')
        instName = instName.replace('.', '_')
        #print a message showing what we are processing
        print('\nProcessing ' + instName)
        # generate timestamp and value field (column) names
        # include the instr name in the timestamp column label so it can be
        # identified standalone
        tsName = 'timestamp_' + instName
        valName = 'value_' + instName
        # create a new dataframe for the instrument. Use the id index to get
        # all the timestamped values for the current id. No need for the
        # tag name (it is the same for every row, and captured above, so leave
        # it out.
        df_instData = pd.DataFrame(data=df_valData.loc[(instId, ), headerList[4]:]) 
        # label the timestamp index column and the value column so the df column
        # names being used to make the TsIdxData match the passed in column names.
        df_instData.index.name = tsName
        df_instData.columns = [valName]
        
        # See if instrument is already in the list. If so append the 
        # data to an existing instrument object already in the object list.
        # If not, then append a new object with the new data to the name and
        # object lists.
        try:
            # get the index in the name list. If it is present, the instrument
            # is already present. Append the data. If it isn't present, this 
            # will throw, and we'll append a new instrument.
            idx = instDataNames.index(instName)
            # Appending the data will apply previously specified value queries
            # and time filtering
            instData[idx].appendData(df_valData)
        except:
            # This instrument is not in the instrument list yet.  
            # Append it to the name list and the object list
            instDataNames.append(instName)
            # Make an object with the instrument name, labels and data frame
            # instrument data object, and append it to the list.
            # Querying of value and filtering of timestamps will happen during
            # construction of the object
            instData.append(TsIdxData(instName, tsName, valName, df_valData,
                                     args.valueQuery, startArg, endArg,
                                     sourceTimeFormat))

        # the instrument data is now captured in the InstData objects.
        # delete the valData dataframe to free up resources.
        del df_instData

    # The value data for all the instruments is now captured in the InstData objects.
    # Delete the valData and the tagList dataframes to free up resources.
    del df_valData
    del df_tagList 

    
elif args.n and len(headerList) >= 3:
    # normalized time data, and there is at least 1 instrument worth of data.
    # The data is expected to have these columns:
    #     [0] Timestamp
    #     [1] Time Bias
    #     [2] Tag 1 Value
    #     ...
    #     [n+1] Tag n Value
    
    # In the normalized time data case, the first column is the timestamp, and 
    # every column after the 3rd is instrument data headered with the instrument
    # name.
    print('\nNormalized Time Data Specified. The source data is expected to \
have the following format:\n \
    TimeStamp, Time Bias, Tag1 Value, Tag2 Value, Tag 3 Value ... ')
    # TODO: Time Bias support
    # KLUDGE: Drop the time bias as the times are already in local time, which 
    # is what is desired. May be better to do somethign smarter with it, like
    # modify the TsIdxData object to be UTC and timezone aware.
    # Timestamps are in local time. No need for hte bias column. Drop it.
    # NOTE: This is needed in the merge sections also!!
    df_source.drop(columns=[headerList[1]], inplace=True, errors='ignore')

    # Index the source data time stamp column. Since we know this will be the
    # index in this case, do this early so we can take advantage of it later.
    # First make sure the timesamp can be converted to a datetime.
    tsName = df_source.columns[0]
    if 'datetime64[ns]' != df_source[tsName].dtype:
        # For changing to timestamps, coerce option for errors is marking
        # dates after midnight (next day) as NaT.
        # Not sure why. Try it with raise, first, and you get
        # all the values. Put it in a try block, just in case an error is
        # raised.
        try:
            df_source[tsName] = pd.to_datetime(df_source[tsName],
                                            errors='raise',
                                            box = True, 
                                            format=sourceTimeFormat,
                                            exact=False,
                                            #infer_datetime_format = True,
                                            origin = 'unix')
        except:
            print('    WARNING: Problem converting some timestamps from \
the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
            df_source[tsName] = pd.to_datetime(df_source[tsName],
                                            errors='coerce',
                                            box = True, 
                                            infer_datetime_format = True,
                                            origin = 'unix')
    # set the timestamp column to be the index
    df_source.set_index(tsName, inplace=True)
    # sort the index for possible better performance later
    df_source.sort_index(inplace=True)
    print('**** Input Data ****')
    print(df_source)
    
    # If there are files specified to merge, merge them with the input file before 
    # further processing. This file has times and tags that may match or
    # may be additional to the source data. The data merge will make the source
    # data wider if there are new tags, and longer if there are new times. One or
    # both may happen.
    # Merge File 1
    if args.archiveMerge1 is not None:
        try:
            print('Merging file "' + args.archiveMerge1 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge1, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am1/archiveMerge1 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Index the source data time stamp column. Since we know this will be the
        # index in this case, do this early so we can take advantage of it later.
        # First make sure the timesamp can be converted to a datetime.
        tsName = df_merge.columns[0]
        if 'datetime64[ns]' != df_merge[tsName].dtype:
            # For changing to timestamps, coerce option for errors is marking
            # dates after midnight (next day) as NaT.
            # Not sure why. Try it with raise, first, and you get
            # all the values. Put it in a try block, just in case an error is
            # raised.
            try:
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='raise',
                                                box = True, 
                                                format=sourceTimeFormat,
                                                exact=False,
                                                #infer_datetime_format = True,
                                                origin = 'unix')
            except:
                print('    WARNING: Problem converting some timestamps from \
    the am1/archiveMerge1 file "' + args.archiveMerge1 + '".  Timestamps may be \
    incorrect, and/or some rows may be missing.')
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True, 
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)
        print('**** Merge Data indexed ****')
        print(df_merge)
        
        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0)
        print('**** Merged Data ****')
        print(df_merged)

        # Drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged
        
# Merge File 2
    if args.archiveMerge2 is not None:
        try:
            print('Merging file "' + args.archiveMerge2 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge2, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am2/archiveMerge2\
parameter: "' + args.archiveMerge2 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Index the source data time stamp column. Since we know this will be the
        # index in this case, do this early so we can take advantage of it later.
        # First make sure the timesamp can be converted to a datetime.
        tsName = df_merge.columns[0]
        if 'datetime64[ns]' != df_merge[tsName].dtype:
            # For changing to timestamps, coerce option for errors is marking
            # dates after midnight (next day) as NaT.
            # Not sure why. Try it with raise, first, and you get
            # all the values. Put it in a try block, just in case an error is
            # raised.
            try:
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='raise',
                                                box = True, 
                                                format=sourceTimeFormat,
                                                exact=False,
                                                #infer_datetime_format = True,
                                                origin = 'unix')
            except:
                print('    WARNING: Problem converting some timestamps from \
    the am2/archiveMerge2 file "' + args.archiveMerge2 + '".  Timestamps may be \
    incorrect, and/or some rows may be missing.')
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True, 
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)
        print('**** Merge Data indexed ****')
        print(df_merge)
        
        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0)
        print('**** Merged Data ****')
        print(df_merged)

        # Drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged

    # Merge File 3
    if args.archiveMerge3 is not None:
        try:
            print('Merging file "' + args.archiveMerge3 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge3, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am3/archiveMerge3 \
parameter: "' + args.archiveMerge3 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Index the source data time stamp column. Since we know this will be the
        # index in this case, do this early so we can take advantage of it later.
        # First make sure the timesamp can be converted to a datetime.
        tsName = df_merge.columns[0]
        if 'datetime64[ns]' != df_merge[tsName].dtype:
            # For changing to timestamps, coerce option for errors is marking
            # dates after midnight (next day) as NaT.
            # Not sure why. Try it with raise, first, and you get
            # all the values. Put it in a try block, just in case an error is
            # raised.
            try:
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='raise',
                                                box = True, 
                                                format=sourceTimeFormat,
                                                exact=False,
                                                #infer_datetime_format = True,
                                                origin = 'unix')
            except:
                print('    WARNING: Problem converting some timestamps from \
    the am3/archiveMerge3 file "' + args.archiveMerge3 + '".  Timestamps may be \
    incorrect, and/or some rows may be missing.')
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True, 
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)
        print('**** Merge Data indexed ****')
        print(df_merge)
        
        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0)
        print('**** Merged Data ****')
        print(df_merged)

        # Drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged
        
    # Merge File 4
    if args.archiveMerge4 is not None:
        try:
            print('Merging file "' + args.archiveMerge4 + '".')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            df_merge = pd.read_csv(args.archiveMerge4, sep=args.sourceDelimiter,
                            delim_whitespace=False, encoding=args.sourceEncoding,
                            header=0, dtype = str, skipinitialspace=True)
        except:
            print('ERROR opening the file specified with the -am4/archiveMerge4 \
parameter: "' + args.archiveMerge4+ '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Index the source data time stamp column. Since we know this will be the
        # index in this case, do this early so we can take advantage of it later.
        # First make sure the timesamp can be converted to a datetime.
        tsName = df_merge.columns[0]
        if 'datetime64[ns]' != df_merge[tsName].dtype:
            # For changing to timestamps, coerce option for errors is marking
            # dates after midnight (next day) as NaT.
            # Not sure why. Try it with raise, first, and you get
            # all the values. Put it in a try block, just in case an error is
            # raised.
            try:
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='raise',
                                                box = True, 
                                                format=sourceTimeFormat,
                                                exact=False,
                                                #infer_datetime_format = True,
                                                origin = 'unix')
            except:
                print('    WARNING: Problem converting some timestamps from \
    the am4/archiveMerge4 file "' + args.archiveMerge4 + '".  Timestamps may be \
    incorrect, and/or some rows may be missing.')
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True, 
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)
        print('**** Merge Data indexed ****')
        print(df_merge)
        
        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0)
        print('**** Merged Data ****')
        print(df_merged)

        # Drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.
        del df_source
        del df_merge
        df_source = df_merged
        del df_merged
        
        
    # At this point, the source data has this structure
    # idx TimeStamp
    # [0] Tag 1 Value
    # ...
    # [n] Tag n Value,
    # and the header row contains the tag names for the value columns.

    # Update the header list after the merge to make sure new tags are reflected.
    headerList = df_source.columns.values.tolist()
    print('**** Header List ****')
    print(headerList)
    # make sure the data is still sorted by time
    # NOTE: This may be unnecessary
    df_source.sort_index(inplace=True)

    # Loop thru the header list. Get the instrument name, create a data frame for
    # each instrument, get the value to be a float, create a list of
    # instruments and an list of TsIdxData objects (one per instrument).
    # If the instrument name is duplicated, the data sets are
    # merged.
    for idx in range(0, len(headerList)):
        instName = headerList[idx]
        # replace the spaces, hyphens, and periods with underscores
        instName = instName.replace(' ', '_')
        instName = instName.replace('-', '_')
        instName = instName.replace('.', '_')
        # Generate timestamp and value field (column) names.
        # These will be used for the exported data.
        # Include the instr name in the timestamp column label so it can be
        # identified standalone
        tsName = 'timestamp_' + instName
        valName = 'value_' + instName
        # print a message showing what we are processing
        print('\nProcessing ' + instName)
        # Create a new dataframe for the instrument and use the above column names.
        #df_valData = pd.DataFrame(data=df_source.iloc[:,[idx]])
        df_valData = pd.DataFrame(data=df_source.iloc[:,idx])
        # drop rows with NaN values, usually these are from the merge
        df_valData.dropna(axis=0, how='any', inplace=True)
        df_valData.columns = [valName]
        df_valData.index.rename(tsName, inplace=True)

        # change the data type of the value column to a float if needed
        if 'float64' != df_valData[valName].dtype:
            # not a float, but it should be. Change the type
            df_valData[valName] = df_valData[valName].astype('float',errors='ignore')
        print(' **** df_valData ****')
        print(df_valData)
        # Data should already be sorted.
        
        # See if instrument is already in the list. If so append the 
        # data to an existing instrument object already in the object list.
        # If not, then append a new object with the new data to the name and
        # object lists.
        try:
            # get the index in the name list. If it is present, the instrument
            # is already present. Append the data. If it isn't present, this 
            # will throw, and we'll append a new instrument.
            idx = instDataNames.index(instName)
            # Appending the data will apply previously specified value queries
            # and time filtering
            instData[idx].appendData(df_valData)
        except:
            # This instrument is not in the instrument list yet.  
            # Append it to the name list and the object list
            instDataNames.append(instName)
            # Make an object with the instrument name, labels and data frame
            # instrument data object, and append it to the list.
            # Querying of value and filtering of timestamps will happen during
            # construction of the object
            instData.append(TsIdxData(instName, tsName, valName, df_valData,
                                     args.valueQuery, startArg, endArg,
                                     sourceTimeFormat))

    # The data is now in instData in data frames. Done with the source data. Delete it.
    del df_source

print(instData)
quit()

# **** Determine the earliest start time, the latest end time, and the minimum
# frequency for the instruments. These will be used to generate the master time
# series used for merging all the data together.
# As long as there is a list of instrument objects,
# loop thru the instruments and get the first and last datetime
# init the holding areas
startTime= pd.NaT
endTime= pd.NaT
freq= np.NaN
if instData:
    # find the earliest and latest start/end times
    for inst in instData:
        # get the earliest start time
        if not inst.data.empty and not pd.isna(inst.startTs) and pd.isna(startTime):
            # first valid time
            startTime = inst.startTs
        elif not inst.data.empty and not pd.isna(inst.startTs) and not pd.isna(startTime):
            # get min 
            startTime = min(startTime, inst.startTs)

        # get the latest end time
        if not inst.data.empty and not pd.isna(inst.endTs) and pd.isna(endTime):
            # first valid time
            endTime = inst.endTs
        elif not inst.data.empty and not pd.isna(inst.endTs) and not pd.isna(endTime):
            # get the max
            endTime = max(endTime, inst.endTs)

        # get the highest frequency in the form of a time offset
        if not inst.data.empty and not pd.isna(inst.timeOffset) and pd.isna(freq):
            # first valid offset
            freq = inst.timeOffset
        elif not inst.data.empty and not pd.isna(inst.timeOffset) and not pd.isna(freq):
            # get min 
            freq = min(freq, inst.timeOffset)
        
# **** From here on, use the start and end not a time (NaT) check as a check to
# see if there is any data
if not pd.isna(startTime) and not pd.isna(endTime):
    # **** Make the start and end arguments the latest of the first data time
    # and the specified start time, and the end time should be the earliest
    # of the last data time and the specified end time.  The arguments were 
    # processed above, so they are valid or set to None if they were invalid
    # or not specified.
    # Init the arguments to the instrument times from above if nothing was
    # specified.

    if startArg is None:
        startArg = startTime

    if endArg is None:
        endArg = endTime

    # At this point the startArg and endArg have either command line argument
    # specified values or the data based values.  Now determine the resulting
    # combination of argument and data values. Make start the later of the two, and
    # end the earlier of the two. This prevents a bunch of NaN values if the data
    # is inside the argument values
    startTime = max(startTime, startArg)
    endTime = min(endTime, endArg)

    # **** Make sure the resampleArg is either the value specified or the
    # minimum of the instrument data frequencies if nothing was specified.
    if resampleArg is None:
        resampleArg = freq

    # Force the start time to start on clean "origin" time. The time range
    # being merged to needs to start on the same time as the date being merged,
    # or the stats are misleading -- and the resampling function already does
    # this.
    # For example, if '5S' is used, the time being merged to needs to start
    # on a division of 5 seconds, so it needs to start with a timestamp
    # ending in 0 or 5 seconds. 
    startTime = startTime.floor(resampleArg)

    # Print messages so we can see what is going to happen.
    print('\n**** Initial processing of each instrument done. For the generated \
dataset:')
    print('    The start time is:', startTime)
    print('    The end time is:', endTime)
    print('    The sampling frequency is:', resampleArg)
    print('    Note that the start and end times are the earliest and latest \
found in the data unless the start and/or end time options are used.')
    print('    Note that the sample frequency used it the highest found in \
the data unless the resampling option is used.\n')

    # **** Create a daterange data frame to act as the master datetime range.
    # Use the above determined start, end, and frequency
    # The data will get left merged using this data frame for time
    # create the timestamp column name
    ts_name = 'timestamp'
    # using the start and end times, build an empty  dataframe with the 
    # date time range as the index. Default sample period to 1 Sec
    try:
        df_dateRange = pd.DataFrame({ts_name:pd.date_range(startTime,
                                                           endTime,
                                                           freq=resampleArg)})
    except:
        print('ERROR: Problem with generated date/time range. Check the \
resample argument.')
        print('Error: ', sys.exc_info())
        quit()

    # Make sure the date range is sorted. This is needed for the
    # merge to work as expected.
    df_dateRange.sort_values(ts_name, ascending=True, inplace=True)
    # set the timestamp as the index
    df_dateRange.set_index(ts_name, inplace=True)

    # **** Populate the destination data frame
    # As long as there is a list of instrument objects,
    # create a new file, erasing any existing with the same name,
    # insert the export compliance message (if not shut off), and then
    # loop thru the instruments and merge the data into the destination data frame
    if instData:
        df_dest = df_dateRange
        # create a new file for writing, deleting any existing version
        try:
            outFile = open(args.outputFileName, 'w', encoding=args.destEncoding)
        except:
            print('ERROR opening the output file. Nothing written.')
            quit()

        # generate the export compliance warning, unless explicitly omitted
        if not args.noExportMsg:
            expCompWarn = \
['WARNING - This document contains technical data export of which',
'is restricted by the Export Administration Regulations (EAR).',
'Release of this document is only authorized for the use of the',
'Institute of Plasma Physics the Chinese Academy of Sciences (ASIPP)',
'the ITER Organization and its duly ratified member nations and their technical',
'representatives for the development of fusion energy for peaceful purposes',
'as defined under export license D513109.',
'Violations of these export laws and regulations are subject to severe',
'civil and criminal penalties.\n\n']

            # write to the output file
            print('**** Writing the output file\n')
            csv.register_dialect('csvDialect', escapechar=' ',
                                lineterminator='\n', quoting=csv.QUOTE_NONE)
            csvWriter = csv.writer(outFile, dialect='csvDialect')
            for row in expCompWarn:
                print(row)
                csvWriter.writerow([row])

        # append the instrument data to the destination data frame.
        # This is where it all comes together ...
        for inst in instData:
            # first, resample the instrument data if it needs to be
            inst.resample(resampleArg, stats)
            # Merge the instrument data with the master dataframe.
            # The backward direction means to take the last instrument value
            # that is on or before the master date range -- i.e. when merging
            # to a time not in instrument time, look backward in time to get
            # the last instrument value 
            # NOTE: Steps were taken during construction to round times to
            # the nearest msec, so fractional msecs do not affect the merge.
            df_dest = pd.merge_asof(df_dest, inst.data,
                                    left_index = True, right_index = True,
                                    direction = 'backward')

        # replace any NaN values in the resulting data frame with 0s so data users
        # are not tripped up with NaN
        df_dest.fillna(0.0, inplace = True)

        try:
            # **** Write the destination data frame to the output file
            # Include frac sec if frequency is < 1 sec
            if resampleArg < to_offset('S'):
                df_dest.to_csv(outFile, sep=args.destDelimiter,
                               encoding=args.destEncoding,
                               date_format ='%Y-%b-%d %H:%M:%S.%f')
            else:
                # no need for fractional sec
             df_dest.to_csv(outFile, sep=args.destDelimiter,
                           encoding=args.destEncoding,
                           date_format ='%Y-%b-%d %H:%M:%S')
        except:
            print('\nERROR writing data to the file. Output file content is suspect.\n')
            print('Error: ', sys.exc_info())
        outFile.close()
    else:
        print('ERROR: No instrument data found. Nothing written\n')

else:
    print('ERROR: No data found. Nothing written\n')

#get end  processing time
procEnd = datetime.now()
print('\n**** End Processing ****')
print('    Process end time: ' + procEnd.strftime('%m/%d/%Y %H:%M:%S'))
print('    Duration: ' + str(procEnd - procStart) + '\n')
