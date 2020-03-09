#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# ftArchPostProc.py
#
# Final Test Archive Data Post Processing
# This program accepts an input csv file, post processes it, and creates a csv
# output file.  An export control message is included at the head of the output
# file, unless the -noExportMsg argument is used.
#
# Given an input file, the program will produce a *.csv file with the name
# specified as the outputFileName with the format:
#   Timestamp, Tag1 Value, Tag2 Value ...
# where the column names are the tag names, and the columns are
# ordered by name

# In the case of a historical trend generated file (the -t command line argument),
# the data columns are as follows:
#   Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
# and the timestamps are not synchronized.  If a time format is not specified
# with the -stf option, then the format is assumed to be MMuu-DD-YYYY hh:mm:ss am/pm.

# In the case of a archive export file (the -a command line argument), the data
# columns are as follows:
#   ValueId,Timestamp,value,quality,flags and there are
# normally multiple valueIDs each at multiple timestamps. If a time format is not
# specified with the -stf option, then the format is assumed to be YYYY-MM-DD
# HH:mm::ss.mmm.

# In the case of a time normalized export file (the -n command line argument), the
# data columns are as follows: Timestamp, Tag1 Value, Tag2 Value, Tag3 Value ... If
# a time format is not specified with the -stf option, then the format is assumed
# to be YYYY-MM-DD HH:mm:ss.mmm.
#
# In the case of a strain gauge export file (the -s command line argument), the
# data file has several rows of header information, followed by data columns
# that are organized are as follows:
# Sample ID, Time offset from start time, Tag1 Value, Tag2 Value, Tag3 Value ...
# In the rows of header data, most importantly is the start time, the tag names,
# and measurment units.  The value timestamp is derived from the start time in
# the header, and the time offset from the row data.
# If a time format is not specified with the -stf option, then the start time
# format is assumed to be MM/DD/YYYY HH:mm:ss am/pm.
#
# Note: The -h, -n, -a, and -s options are mutually exclusive. One and only one must
# be specified.
#
# Field delimiters can be specified for the input and output files. The
# default field delimiter is the comma (","). If another delimiter needs to
# be specified, it can be done so using the -sd, -sourceDelimiter, -dd, or
# -destDelimiter options. If more than one character is specified, the
# delimiter will be interpreted as a regular expression.
#
# File encoding can be specified for the input and output files. The default
# encoding is "utf-8". If another encoding needs to be specified, it can be
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
# -t, (required and mutually exclusive with -a -s and -n).  Input file
# is a historical trend export file.  The format is:
#  Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
# where timestamp default format is m/d/yyyy hh:mm:ss am/pm
#
# -a (required and mutually exclusive with -t -s and -n). Input file is a
# archive export file. The format is:
#   TagId, TagName, Timestamp (YYYY-MM-DD HH:MM:SS.mmm), DataSource, Value, Quality
# where timestamp default format is yyyy-mm-dd hh:mm:ss.nnn (24 hr)
#
# -s (required and mutually exclusive with -a, -t, and -n). Input file is a
# strain gauge data file. The format is:
#   the data file has several rows of header information, followed by data columns
# that are organized are as follows:
# Sample ID, Time offset from start time, Tag1 Value, Tag2 Value, Tag3 Value ...
# where start time default format is m/d/yyyy hh:mm:ss am/pm
#
# -n (required and mutually exclusive with -t -a and -s). Input file is a time
# normalized export file.  The format is:
#   Timestamp, Time Bias, Tag1 Value, Tag2 Value, Tag3 Value ...
# where timestamp default format is yyyy-mm-dd hh:mm:ss.nnn (24 hr)
#
# -am1, -am2, -am3, -am4 or --archiveMerge (optional, default=None). Archive Merge.
# Merge these named files with the data in the inputFileName before processing.
# Must have the same format/layout as the input file.
#
# -se or --sourceEncoding (optional, default of "utf-8"). Source file encoding.
#
# -sd or --sourceDelimiter (optional, default of ","). Destination file field
# delimiter. Single character or regex.
#
# -dd or --destDelimiter (optional, default of ","). Destination file field
# delimiter. Single character or regex.
#
# -de or --destEncoding (optional, default of "utf-8"). Destination file encoding.
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
# -stf or --sourceTimeFormat (optional, default=None) Specify the format of the
# source data time format, as a string. Use the following placeholders: %m minutes,
# %d days, %Y 4 digit year, %y two digit year, %H hours (24hr format), %I hours (12
# hr format), %M minutes, %S seconds, %p AM/PM. If no format is specified, than the
# format is determined by the -t, -a, or -n option.
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
# -v, --verbose (optional, defalt=False). Increse output Messaging.

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

# user libraries
# Note: May need PYTHONPATH (set in ~/.profile?) to be set depending
# on the location of the imported files
# TimeStamped Indexed Data Class
from bpsTsIdxData import TsIdxData
# list duplication helper functions
from bpsListDuplicates import listDuplicates
from bpsListDuplicates import listToListIntersection


# **** argument parsing
# define the arguments
# create an epilog string to further describe the input file
eplStr="""Final Test Archive Data Post Processing
 This program accepts an input csv file, post processes it, and creates a csv
 output file.  An export control message is included at the head of the output
 file, unless the -noExportMsg argument is used.

 Given an input file, the program will produce a *.csv file with the name
 specified as the outputFileName with the format:
   Timestamp, Tag1 Value, Tag2 Value ...
 where the column names are the tag names, and the columns are
 ordered by name

 In the case of a historical trend generated file (the -t command line argument),
 the data columns are as follows:
   Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
 and the timestamps are not synchronized.  If a time format is not specified
 with the -stf option, then the format is assumed to be MM/DD/YYYY hh:mm:ss am/pm.

 In the case of a archive export file (the -a command line argument), the data
 columns are as follows:
   ValueId,Timestamp,value,quality,flags and there are
 normally multiple valueIDs each at multiple timestamps. If a time format is not
 specified with the -stf option, then the format is assumed to be YYYY-MM-DD
 HH:mm::ss.mmm.

 In the case of a time normalized export file (the -n command line argument), the
 data columns are as follows: Timestamp, Tag1 Value, Tag2 Value, Tag3 Value ... If
 a time format is not specified with the -stf option, then the format is assumed
 to be YYYY-MM-DD HH:mm:ss.mmm.

 In the case of a displaceent export file (the -s command line argument), the
 data file has several rows of header information, followed by data columns
 that are organized are as follows:
 Sample ID, Time offset from start time, Tag1 Value, Tag2 Value, Tag3 Value ...
 In the rows of header data, most importantly is the start time, the tag names,
 and measurment units.  The value timestamp is derived from the start time in
 the header, and the time offset from the row data.
 If a time format is not specified with the -stf option, then the start time
 format is assumed to be MM/DD/YYYY HH:mm:ss am/pm.

 Note: The -h, -n, -a, and -s options are mutually exclusive. One and only one must
 be specified.

 Field delimiters can be specified for the input and output files. The
 default field delimiter is the comma (","). If another delimiter needs to
 be specified, it can be done so using the -sd, -sourceDelimiter, -dd, or
 -destDelimiter options. If more than one character is specified, the
 delimiter will be interpreted as a regular expression.

 File encoding can be specified for the input and output files. The default
 encoding is "utf-8". If another encoding needs to be specified, it can be
 done using the -se, -sourceEncoding, -de, or -destEncoding options.

 It is assumed that the first row is a header. In the case of a historical
 trend input file (-t option), the tag names are derrived from the header.
 In the case of a archive data input file (-a option), the tag names are
 pulled from the data.


 Command line arguments are:
 inputFileName (required, positional). The source data csv file.

 outputFileName (required, positional). The .csv output file name.

 -t, (required and mutually exclusive with -a -s and -n).  Input file
 is a historical trend export file.  The format is:
  Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...

 -a (required and mutually exclusive with -t -s and -n). Input file is a
 archive export file. The format is:
   TagId, TagName, Timestamp (YYYY-MM-DD HH:MM:SS.mmm), DataSource, Value, Quality

 -s (required and mutually exclusive with -a, -t, and -n). Input file is a
 strain gauge data file. The format is:
   the data file has several rows of header information, followed by data columns
 that are organized are as follows:
 Sample ID, Time offset from start time, Tag1 Value, Tag2 Value, Tag3 Value ...

 -n (required and mutually exclusive with -t -a and -s). Input file is a time
 normalized export file.  The format is:
   Timestamp, Time Bias, Tag1 Value, Tag2 Value, Tag3 Value ...

 -am1, -am2, -am3, -am4 or --archiveMergen (optional, default=None). Archive Merge.
 Merge these named files with the data in the inputFileName before processing.
 Must have the same format/layout as the input file.

 -se or --sourceEncoding (optional, default of "utf-8"). Source file encoding.

 -sd or --sourceDelimiter (optional, default of ","). Destination file field
 delimiter. Single character or regex.

 -dd or --destDelimiter (optional, default of ","). Destination file field
 delimiter. Single character or regex.

 -de or --destEncoding (optional, default of "utf-8"). Destination file encoding.

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

 -stf or --sourceTimeFormat (optional, default=None) Specify the format of the
 source data time format, as a string. Use the following placeholders: %m minutes,
 %d days, %Y 4 digit year, %y two digit year, %H hours (24hr format), %I hours (12
 hr format), %M minutes, %S seconds, %p AM/PM. If no format is specified, than the
 format is determined by the -t, -a, -s, or -n option.

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

 -stats (optional, default='m') Choose which statistics to calculate when
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
 message is included unless this argument is specified.

 -v, --verbose (optional, defalt=False). Increse output Messaging. """

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
parser.add_argument('-sd', '--sourceDelimiter', default=',', metavar='', \
                   help='Source file field delimiter. Default is a comma (\",\").')
parser.add_argument('-se', '--sourceEncoding', default='utf_8', metavar='', \
                   help='Source file encoding. Default is utf_8.')
parser.add_argument('-dd', '--destDelimiter', default=',', metavar='', \
                   help='Destination file field delimiter. Default is a comma (\",\").')
parser.add_argument('-de', '--destEncoding', default='utf_8', metavar='', \
                   help='Source file encoding. Default is utf_8.')
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
                    default=None, metavar='', \
                    help='Specify the format of the source data time format, \
as a string. Use the following placeholders:%%m month, %%d day, %%Y 4 digit \
year, %%y two digit year, %%H hour (24hr format) %%I hour (12 hr format), %%M \
minute, %%S second, %%f for fractional seconds (e.g. %%S.%%f), %%p AM/PM. \
If no format is specified, than the format is determined by the -t, -a, or -n \
option.')
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

parser.add_argument('-v', '--verbose', action='store_true', default=False, \
                    help='Increase output messages.')

# add -t -a -n and -s as a required, but in a mutually exclusive group
typegroup = parser.add_mutually_exclusive_group(required=True)
typegroup.add_argument('-t',  action='store_true', default=False, \
                    help='Historical trend input file type (format).')
typegroup.add_argument('-a', action='store_true', default=False, \
                    help='Archive data input file type (format).')
typegroup.add_argument('-n', action='store_true', default=False, \
                    help='Time normalized input file type (format).')
typegroup.add_argument('-s', action='store_true', default=False, \
                    help='Strain gauge input file type (format).')
# parse the arguments
args = parser.parse_args()

# At this point, the arguments will be:
# Argument          Values      Description
# args.inputFileName    string file to get data from
# args.outputFileName   string file to write processed data to
# args.archiveMerge1    string file to merge with input
# args.archiveMerge2    string file to merge with input
# args.archiveMerge2    string file to merge with input
# args.archiveMerge4    string file to merge with input
# args.sourceDelimiter  string Input file field delimiter. Default is ","
# args.sourceEncoding   string Input file encoding. Default is utf_8.
# args.destDelimiter    string Dest file field delimiter. Default is (",")
# args.destEncoding     string Dest file encoding. Default is utf_8.
# args.valueQuery       string Optional query of the data
# args.startTime        string Optional start date time
# args.endTime          string Options end date time
# args.sourceTimeFormat string Format string for source data timestamps
# args.resample         string Resample period. Default is 'S' or 1 sample/sec.
# args.stats            string Stats to calc. Value, min, max, ave, std dev.
# args.noExportMsg      True/False Exclude export control message when set
# args.verbose          True/False Increase output messaging
# args.t                True/False Historical trend input file type when set
# args.a                True/False Archive data input file type when set
# args.n                True/False Time Normalized input file type when set.
# args.s                True/False Strain gauge input file type when set.

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
    except ValueError as ve:
        # not convertable ... invalid ... ignore
        print('WARNING: Invalid start time. Ignoring.')
        print(ve)
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

    except ValueError as ve:
        # not convertable ... invalid ... ignore
        print('WARNING: Invalid end time. Ignoring.')
        print(ve)
        endArg = None

else:
    # arg is none, so update the internal version
    endArg = None

# get the resample argument
if args.resample is not None:
    # a resample arg was supplied.  Try to use it, or default to 1 sec.
    try:
        resampleArg = to_offset(args.resample) # use the offset version
    except ValueError as ve:
        print('WARNING: Invalid resample period specified. Using 1 second')
        print(ve)
        resampleArg = to_offset('S')
else:
    # arg is none, so update the internal version
    resampleArg = None

# force the stats argument to a lower case string so they are case insensitive.
stats = str(args.stats).lower()

# Use the specified argument for stf, or use -t/-a/-n/-s option to
# determine the stf
if args.sourceTimeFormat is not None:
    # a source time has been specified. Use it over the other defaults
    # make sure the source timestamp format argument is a string
    sourceTimeFormat = str(args.sourceTimeFormat)
elif args.t:
    # no format specified. Use the default for this option
    #sourceTimeFormat = '%m/%d/%Y %I:%M:%S %p'
    sourceTimeFormat = '%Y-%m-%d %H:%M:%S.%f'
elif args.a:
    # no format specified. Use the default for this option
    sourceTimeFormat = '%Y-%m-%d %H:%M:%S.%f'
elif args.n:
    # no format specified. Use the default for this option
    sourceTimeFormat = '%Y-%m-%d %H:%M:%S.%f'
elif args.s:
    # no format specified. Use the default for this option
    sourceTimeFormat = '%m/%d/%Y %I:%M:%S %p'

# **** read the csv file into a data frame.
# The first row is treated as the header, except for in the -s case, and then
# the header info is delt with when processing the -s option below.
try:
    # use string as the data type for all columns to prevent automatic
    # datatype detection. We don't know ahead of time how many columns are
    # being read in, so we don't yet know the types.
    # We want duplicate column names to be preserved as in.
    # They will get filtered out as duplicates later.
    # The default behavior of read_csv is to append a ".n" to the column name
    # where n is an integer value starting at 1 and incrementing up for each
    # duplicate found. The problem with this is later, this gets interpreted as
    # a different tag if using some options.
    # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
    # column names by turning off the mangling described above.
    # It is "not supported yet" but is in the documentation for
    # Pandas 0.22 and maybe earler as being a feature!!
    # It throws a ValueError if used.  As a work around, don't use
    # mangle_dupe_cols=False, and use header=None instead of header=0 in the
    # read_csv function.  Then manually rename the columns using the 1st row
    # of the csv.
    df_source = pd.read_csv(args.inputFileName, sep=args.sourceDelimiter,
                        delim_whitespace=False, encoding=args.sourceEncoding,
                        header=None, dtype = str, skipinitialspace=True)
                        # mangle_dupe_cols=False)
    # Manually rename the columns using the 1st row of the csv.
    # Don't do this with the strain gauge file types because the header
    # processing is more complicated, and is done below.
    if not args.s:
        df_source = df_source.rename(columns=df_source.iloc[0], copy=False).iloc[1:].reset_index(drop=True)
    # NOTE: At this point the source may have duplicate columns. This may be okay
    # or it may be problematic, depending on the -t, -a, -s or -n option. Deal with
    # duplicates below when we check the option.

except ValueError as ve:
    print('ERROR opening source file: "' + args.inputFileName + '". Check file \
name, file presence, and permissions. Unexpected encoding can also cause this \
error.')
    print(ve)
    quit()

# print diagnostic info if verbose is set
if args.verbose:
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
# Note that in the -s strain gauge data, the headerList or column names are not
# yet processed, so the column names and header list are just 0, 1, ..., n
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
and the timestamps may or may not be synchronized.\n')

    # Deal with duplicates in the input file.
    # Duplicates with this data format may or may not be problematic since
    # a tag may be represented more than once, but contain different timestamps.
    # We don't want to throw away a column just becasue it has a duplicate name.
    # If the timestamps are duplicated, dups will be dropped after merging.
    # Detect duplicates and warn.
    dups = listDuplicates(df_source)
    if dups:
        # duplicates have been found.  Notify and continue.
        print('    WARNING: There are column names duplicated in the input file "' + args.inputFileName + '".\n\
This is allowed, but if the duplicate column or columns contain duplicate timestamps,\n\
then only one value will be retained.\nThe following column names are duplicated:')
        print(dups)
        print()

    # Define a function to merge specified files. Put the internal definition
    # here, as if only pertains to this file type, and so it can be seen
    # before use
    def aMerge(fileToMerge, sep, encoding, df_src):
        try:
            print('Merging file "' + fileToMerge + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(fileToMerge, sep=sep,
                                delim_whitespace=False, encoding=encoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR when trying to merge: "' + fileToMerge + '".\n \
Error opening file. Check file name, file presence, and permissions. \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()

        # Deal with duplicates in the merge file.
        # Detect duplicates and warn.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are column names duplicated in the file "' + fileToMerge + '.\n \
This is allowed, but if the duplicate column or columns \
contain duplicate timestamps, then only one value will be retained. \n \
The following tags column names are duplicated:')
            print(dups)
            print()

        # Deal with duplicates between the source and merge file.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_src, df_merge)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the merge file "' + fileToMerge + '".\n\
This is allowed, but if the duplicate column or columns contain duplicate timestamps,\n\
then only one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append columns (axis = 1), keeping the header rows.
        # There may be NaN values present when/if columns are diffenrent length.
        # This isn't different than in the input file.
        df_merged = pd.concat([df_src, df_merge], axis=1, join='outer', sort=False)
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
            print('**** Merged Data ****')
            print(df_merged)

        del df_src
        del df_merge
        df_src = df_merged
        del df_merged


    # If there are files specified to merge, merge them with the input file before
    # further processing. Since this file format has independent time/value pairs
    # in columns going to the right, this merge simply makes the source data wider
    # by appending columns (pd.concat with axis=1).
    # Merge File 1
    if args.archiveMerge1 is not None:
        aMerge(args.archiveMerge1, sep=args.sourceDelimiter, encoding=args.sourceEncoding, df_src=df_source)

    # Merge File 2
    if args.archiveMerge2 is not None:
        aMerge(args.archiveMerge2, sep=args.sourceDelimiter, encoding=args.sourceEncoding, df_src=f_source)

    # Merge File 3
    if args.archiveMerge3 is not None:
        aMerge(args.archiveMerge3, sep=args.sourceDelimiter, encoding=args.sourceEncoding, df_src=df_source)

    # Merge File 4
    if args.archiveMerge4 is not None:
        aMerge(args.archiveMerge4, sep=args.sourceDelimiter, encoding=args.sourceEncoding, df_src=df_source)

    # update the header list after the merge to make sure new tags are reflected.
    headerList = df_source.columns.values.tolist()

    # Loop thru the header list. Get the instrument name, create a data frame for
    # each instrument, get the timestamp to be a datetime and the value to be a
    # float, create a list of instruments and an list of TsIdxData objects (one
    # per instrument).  If the instrument name is duplicated, the data sets are
    # merged.
    for idx in range(0, len(headerList), 2):
        # For each header entry, make instrument and timestamp column names.
        # Even indexes are timestamps, odd indexes are values.
        # Get the inst name, leaving off the bit after the last space, which is
        # normally 'Time' or 'ValueY'. If there is no space found, use the entire
        # string as the instrument name.
        # rpartition separates a string at the last separator,
        # working from the right. Given a separator, rpartition returns a tuple:
        #   The part before the separator, the separator, and the part after
        #   the separator.  If the separator is not found, then the first two
        #   elements are empty, and the third member contains the original string.
        # member as the tag name -- this allows tag names with spaces to be
        # preserved.
        #
        separated = headerList[idx].rpartition(' ')
        if not separated[0]:
            # No spaces in the header entry. Use the entire thing as the inst name.
            instName = separated[2]
        else:
            # Use the first part before the leftmost space as the inst name
            instName = separated[0]

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
        # Create a new instrument object and use the above column names.
        tid_inst = TsIdxData(instName, tsName, valName,
                             df_source.iloc[:,[idx,idx+1]],
                             args.valueQuery, startArg, endArg,
                             sourceTimeFormat, forceColNames=True)
        # See if instrument is already in the list. If so append the
        # data to an existing instrument object already in the object list.
        # If not, then append a new object with the new data to the name and
        # object lists.
        if instName in instDataNames:
            # An instrument with the same name already exists.
            # Append this data to it
            idx = instDataNames.index(instName)
            print('Inst in list at index ' + str(idx) + '. Appending data.')

            # Appending the data will apply previously specified value queries
            # and time filtering
            instData[idx].appendData(tid_inst.data, 0) # don't ignore any rows
        else:
            # This instrument is not in the instrument list yet.
            # Append it to the name list and the object list
            print('Inst not yet seen. Appending new instrument to list of instruments.')
            instDataNames.append(instName)
            # Make an object with the instrument name, labels and data frame
            # instrument data object, and append it to the list.
            # Querying of value and filtering of timestamps will happen during
            # construction of the object
            instData.append(tid_inst)

        # The instrument data is now contained in the instrument InstData object.
        # Delete the instrument object to free up resources.
        del tid_inst

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
follows:\n    TagId, TagName, Timestamp (YYYY-MM-DD HH:MM:SS.mmm), DataSource, Value, Quality\n\
Where normally there are multiple tags each at multiple timestamps. Timestamps are \
not necessarily synchronized.\n')

    # Deal with duplicates in the input file.
    # Duplicates with this data format within the same file are problematic
    # because they don't make sense, and are an indicator of invalid source
    # data. Message out and punt.
    dups = listDuplicates(df_source)
    if dups:
        # duplicates have been found.  Notify leave.
        print('    ERROR: There are column names duplicated in the input file "' + args.inputFileName + '".\n\
This is not allowed with this type of data because it does not make sense.\n\
There will be no further processing.\nThe following column names are duplicated:')
        print(dups)
        quit()

    # If there are files specified to merge, merge them with the input file before
    # further processing. Since the file format has rows being unique on
    # TagId and Timestamp combo, this merge simply makes the source data longer
    # by appending rows (pd.concat with axis=0).
    # Merge File 1
    if args.archiveMerge1 is not None:
        try:
            print('Merging file "' + args.archiveMerge1 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge1, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('    ERROR opening the file specified with the -am1/archiveMerge1 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they don't make sense, and are an indicator of invalid source
        # data. Message out and punt.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge1 + '" specified \
with the -am1/archiveMerge1 parameter.\nThis is not allowed with this type of data because it does not make sense.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # This is expected (necessary) with this data type. Nothing to do.

        # Now merge the data. Append rows (axis = 0), ignoring overlapping
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0,
                              ignore_index=True, join='outer', sort=False)
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.

        # print diagnostic info if verbose is set
        if args.verbose:
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
            print('Merging file "' + args.archiveMerge2 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge2, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am2/archiveMerge2 \
parameter: "' + args.archiveMerge2 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they don't make sense, and are an indicator of invalid source
        # data. Message out and punt.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge2 + '" specified \
with the -am2/archiveMerge2 parameter.\nThis is not allowed with this type of data because it does not make sense.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # This is expected (necessary) with this data type. Nothing to do.

        # Now merge the data. Append rows (axis = 0), ignoring overlapping
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0, \
                              ignore_index=True, join='outer', sort=False)
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.

        # print diagnostic info if verbose is set
        if args.verbose:
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
            print('Merging file "' + args.archiveMerge3 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge3, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am3/archiveMerge3 \
parameter: "' + args.archiveMerge3 + '".\n Check file name, file presence, and permissions. \
Unexpected encoding can also cause this error.')
            quit()

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they don't make sense, and are an indicator of invalid source
        # data. Message out and punt.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge3 + '" specified \
with the -am3/archiveMerge3 parameter.\nThis is not allowed with this type of data because it does not make sense.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # This is expected (necessary) with this data type. Nothing to do.

        # Now merge the data. Append rows (axis = 0), ignoring overlapping
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0, \
                              ignore_index=True, join='outer', sort=False)
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.

        # print diagnostic info if verbose is set
        if args.verbose:
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
            print('Merging file "' + args.archiveMerge4 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge4, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am4/archiveMerge4 \
parameter: "' + args.archiveMerge4 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they don't make sense, and are an indicator of invalid source
        # data. Message out and punt.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge4 + '" specified \
with the -am4/archiveMerge4 parameter.\nThis is not allowed with this type of data because it does not make sense.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # This is expected (necessary) with this data type. Nothing to do.

        # Now merge the data. Append rows (axis = 0), ignoring overlapping
        # index (row numbers)
        df_merged = pd.concat([df_source, df_merge], axis=0, \
                              ignore_index=True, join='outer', sort=False)
        # drop the source and make the merged data the new source, then drop the merged data
        # This is so follow on code always has a valid df_source to work with, just as if
        # no files were merged.

        # print diagnostic info if verbose is set
        if args.verbose:
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
    except ValueError as ve:
        print('    WARNING: Problem converting some timestamps from \
the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
        print(ve)
        df_valData[headerList[2]] = pd.to_datetime(df_valData[headerList[2]],
                                                   errors='coerce',
                                                   box = True,
                                                   infer_datetime_format = True,
                                                   origin = 'unix')


    # Remove any NaN/NaT values as a result of conversion
    df_valData.dropna(subset=[headerList[2]], how='any', inplace=True)
    # Rround the timestamp to the nearest ms. Unseen ns and
    # fractional ms values are not always displayed, and can cause
    # unexpected merge and up/downsample results.
    try:
        df_valData[headerList[2]] = df_valData[headerList[2]].dt.round('L')
    except ValueError as ve:
        print('    WARNING: Timestamp cannot be rounded.')
        print(ve)

    # Get rid of any duplicate timestamps. Done after rounding in case rouding
    # introduced dups.
    df_valData.drop_duplicates(subset=[headerList[0],headerList[2]],
                               keep='last', inplace=True)
    # now set the index to a multi-index of TagId,Timestamp
    df_valData.set_index([headerList[0],headerList[2]], inplace=True)
    # sort the index for possible better performance later
    df_valData.sort_index(inplace=True)

    # print diagnostic info if verbose is set
    if args.verbose:
        print('**** df_valData ****')
        print(df_valData)

    # Now create a dataframe to hold a unique list of tag ids and tag names.
    df_tagList = df_source.drop(columns=[headerList[2],headerList[3],
                                         headerList[4],headerList[5]],
                                inplace=False, errors='ignore')
    # force the id column datatype to an int
    df_tagList[headerList[0]] = df_tagList[headerList[0]].astype('int',errors='ignore')
    # drop any NaN/NaT values
    df_tagList.dropna(how='any', inplace=True)
    # Drop the duplicate ids
    df_tagList.drop_duplicates(subset=headerList[0], keep='first', inplace=True)
    # Set the index to the tagId
    df_tagList.set_index(headerList[0], inplace=True)
    # sort the index for possible better performance later
    df_tagList.sort_index(inplace=True)

    # print diagnostic info if verbose is set
    if args.verbose:
        print('**** df_tagList ****')
        print(df_tagList)

    # done with the source data. Delete it.
    del df_source

    # Now we have a sorted list of tags and a dataframe full of values.
    # Go thru the tag list and make an intrument for each tag with the data.
    # The TsIdxData object is used for each instruments data.
    for trow in df_tagList.itertuples(index=True, name=None):
        # itertuples will return a tuple (id, tagname) for a row
        # For tag, get the id and the name.
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
        # Create a new instrument object and use the above column names.
        # Use the id index to get all the timestamped values for the current
        # id. No need for the tag name (it is the same for every row, and
        # captured above, so leave it out.
        tid_inst = TsIdxData(instName, tsName, valName,
                             df_valData.loc[(instId, ), headerList[4]:],
                             args.valueQuery, startArg, endArg,
                             sourceTimeFormat, forceColNames=True)

        # See if instrument is already in the list. If so append the
        # data to an existing instrument object already in the object list.
        # If not, then append a new object with the new data to the name and
        # object lists.
        if instName in instDataNames:
            # An instrument with the same name already exists.
            # Append this data to it
            idx = instDataNames.index(instName)
            print('Inst in list at index ' + str(idx) + '. Appending data.')
            # Appending the data will apply previously specified value queries
            # and time filtering
            instData[idx].appendData(tid_inst.data, 0) # don't ignore any rows
        else:
            # This instrument is not in the instrument list yet.
            # Append it to the name list and the object list
            print('Inst not yet seen. Appending new instrument to list of instruments.')
            instDataNames.append(instName)
            # Make an object with the instrument name, labels and data frame
            # instrument data object, and append it to the list.
            # Querying of value and filtering of timestamps will happen during
            # construction of the object
            instData.append(tid_inst)

        # the instrument data is now captured in the InstData objects.
        # delete it to free up resources.
        del tid_inst

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
have the following format:\n\
    TimeStamp, Time Bias, Tag1 Value, Tag2 Value, ...\n')
    # TODO: Time Bias support
    # KLUDGE: Drop the time bias as the times are already in local time, which
    # is what is desired. May be better to do something smarter with it, like
    # modify the TsIdxData object to be UTC and timezone aware.
    # Timestamps are in local time. No need for the bias column. Drop it.
    # NOTE: This is needed in the merge sections also!!
    df_source.drop(columns=[headerList[1]], inplace=True, errors='ignore')

    # Deal with duplicates in input file.
    # Duplicates with this data format within the same file are problematic
    # because they represent a tag with more than one value at the same timestamp.
    # While this could be delt with when merging, it is an indication that the
    # data may not be as expected. Error out so a person needs to take a look.
    dups = listDuplicates(df_source)
    if dups:
        # duplicates have been found.  Notify leave.
        print('    ERROR: There are column names duplicated in the input file "' + args.inputFileName + '".\n\
This is not allowed with this type of data because it usually means ambiguous tag values.\n\
There will be no further processing.\nThe following column names are duplicated:')
        print(dups)
        quit()

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
        except ValueError as ve:
            print('    WARNING: Problem converting some timestamps from \
the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
            print(ve)
            df_source[tsName] = pd.to_datetime(df_source[tsName],
                                            errors='coerce',
                                            box = True,
                                            infer_datetime_format = True,
                                            origin = 'unix')
    # Remove any NaN/NaT values as a result of conversion
    df_source.dropna(subset=[tsName], how='any', inplace=True)
    # Rround the timestamp to the nearest ms. Unseen ns and
    # fractional ms values are not always displayed, and can cause
    # unexpected merge and up/downsample results.
    try:
        df_source[tsName] = df_source[tsName].dt.round('L')
    except ValueError as ve:
        print('    WARNING: Timestamp cannot be rounded.')
        print(ve)

    # Get rid of any duplicate timestamps. Done after rounding in case rounding
    # introduced dups.
    df_source.drop_duplicates(subset=[tsName], keep='last', inplace=True)
    # set the timestamp column to be the index
    df_source.set_index(tsName, inplace=True)
    # sort the index for possible better performance later
    df_source.sort_index(inplace=True)

    # If there are files specified to merge, merge them with the input file before
    # further processing. This file has times and tags that may match or
    # may be additional to the source data. The data merge will make the source
    # data wider if there are new tags, and longer if there are new times. One or
    # both may happen.
    # Merge File 1
    if args.archiveMerge1 is not None:
        try:
            print('Merging file "' + args.archiveMerge1 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge1, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am1/archiveMerge1 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()

        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge1 + '" specified \
with the -am1/archiveMerge1 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge1 + '" specified with the -am1/archiveMerge1 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
            print('Merging file "' + args.archiveMerge2 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge2, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am2/archiveMerge2\
parameter: "' + args.archiveMerge2 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge2 + '" specified \
with the -am2/archiveMerge2 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge2 + '" specified with the -am2/archiveMerge2 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
            print('Merging file "' + args.archiveMerge3 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each 
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for 
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use 
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge3, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am3/archiveMerge3 \
parameter: "' + args.archiveMerge3 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge3 + '" specified \
with the -am3/archiveMerge3 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge3 + '" specified with the -am3/archiveMerge3 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
            print('Merging file "' + args.archiveMerge4 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge4, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am4/archiveMerge4 \
parameter: "' + args.archiveMerge4+ '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge4 + '" specified \
with the -am4/archiveMerge4 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n \
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge4 + '" specified with the -am4/archiveMerge4 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
    # and the header contains the tag names for the value columns.

    # Update the header list after the merge to make sure new tags are reflected.
    headerList = df_source.columns.values.tolist()

    # Make sure the data is still sorted by time after the merge. This may be
    # unnecessary, but just in case.
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
        # Create a new instrument object and use the above column names.
        tid_inst = TsIdxData(instName, tsName, valName,
                             df_source.iloc[:,idx],
                             args.valueQuery, startArg, endArg,
                             sourceTimeFormat, forceColNames=True)
        # See if instrument is already in the list. If so append the
        # data to an existing instrument object already in the object list.
        # If not, then append a new object with the new data to the name and
        # object lists.
        if instName in instDataNames:
            # An instrument with the same name already exists.
            # Append this data to it
            idx = instDataNames.index(instName)
            print('Inst in list at index ' + str(idx) + '. Appending data.')

            # Appending the data will apply previously specified value queries
            # and time filtering
            instData[idx].appendData(tid_inst.data, 0) # don't ignore any rows
        else:
            # This instrument is not in the instrument list yet.
            # Append it to the name list and the object list
            print('Inst not yet seen. Appending new instrument to list of instruments.')
            instDataNames.append(instName)
            # Make an object with the instrument name, labels and data frame
            # instrument data object, and append it to the list.
            # Querying of value and filtering of timestamps will happen during
            # construction of the object
            instData.append(tid_inst)

        # The instrument data is now contained in the instrument InstData object.
        # Delete the instrument object to free up resources.
        del tid_inst

    # The data is now in instData in data frames. Done with the source data. Delete it.
    del df_source

elif args.s and len(headerList) >= 3:
    # Strain gauge data, and there are at least three cols.
    # The header has this structure:
    # Label                 Column                  Information
    # "Scan Session:"       0                       Informational.
    #                                               Not used for processing
    # "Start Time:"         0                       Data start time.
    #                                               Data times are elapsed from this
    # "Assignment:"         0:label, 2-n: names     tag names
    # "Reduction Method:"   0:label, 2-n: units     units or strain
    #
    # Next row is a header of sorts, but it does not contain tag names, so it
    # won't get used:
    # "ID", "Seconds Elapsed", Units for value 1, ... Units for value n
    #
    # After the header information, the data is expected to have these columns:
    # [0] ID, Sample Id, row number
    # [1] Seconds elapsed (from start time)
    # [2] Tag 1 value
    # ...
    # [n+1] Tag n Value
    #
    # TODO (maybe): Dynamically determie first column whcih contains "ID"
    #       and "...Elapsed"  Use these column numbers to "anchor" the parsing.
    #
    # Interate thru columns and find the "ID" marker (exact match).
    # This is important becuase it marks the row where it and above is the
    # header information and below which is the data.
    # df.shape returns a (row count, column count) tuple
    markLabel = 'ID'
    idCol = -1  # keep track of where mark is found
    idRow = -1
    for colNum in range(df_source.shape[1]):     # 0 to max columns
        # look for the mark (exact match)
        markRows = df_source[colNum].eq(markLabel)
        if markRows.any():
            # The mark was found
            # Keep the row and column, and exit the loop (assume only one 'ID' marker
            idCol = colNum
            idRow = markRows.idxmax()
            break
    # if we get here, and id col/row are < 0, the id mark was not found. Punt
    if idCol < 0 or idRow < 0:
        print('ERROR: Data may be invalid.  Looking for a cell with an exact \
match to "' + markLabel + '". It is used to delimit the header from the data \
and to delimit the leftmost column. Unable to process strain data.')
        quit()

    # Before we deal with the data, we need some important things from
    # the header: the start time, the tag names (Assignment), and the
    # the units (Reduction Method).
    # Note: Use the id column and row determined above for positioning.
    # Assume the header is this row and above, and the meaningful data
    # is from this column and to the right.
    #
    # Get the start time.
    # Look for the label in the left column. If it exists, the resulting series
    # will have a True value in the position corresponding to the row.
    # If found, break out the non-label part and trim off leading/trailing space.
    markLabel = 'Start Time:'
    markRows = df_source[idCol].str.match(markLabel, case=False, na=False)
    if markRows.any():
        # The label was found -- the row will be marked as true, which is considered
        # greater than false, so it will be the index with the max value.
        # Get the contents, which will include the label.
        # Split out the label, trim the other part, and save it
        # as a pandas timestamp
        startTime = df_source[idCol][markRows.idxmax()].split(":", 1)
        try:
            startTime = pd.Timestamp(startTime[1].strip())
        except ValueError as ve:
            print('ERROR: Problem converting Start Time to a timestamp. \
Unable to process strain data.')
            print(ve)
            quit()
    else:
        # no start time found.  Gartz to go...
        print('\nERROR: No Start Time found. Unable to process strain data.')
        quit()

    # Get the tag names (assignments)
    # Look for the label in the left column. If it exists, the resulting series
    # will have a True value in the position corresponding to the row.
    # If found, get the row, removing the first two columns.
    markLabel = 'Assignment:'
    markRows = df_source[idCol].str.match(markLabel, case=False, na=False)
    if markRows.any():
        # The label was found -- the row will be marked as true, which is considered
        # greater than false, so it will be the index with the max value.
        # Get the data frame row for this location, stripping off the first
        # two columns which contain the label and a blank location corresponding
        # to the elapsed time column.
        tagNames = df_source.iloc[markRows.idxmax()][idCol + 2:]
    else:
        # Tag name label not found.  Gartz to go...
        print('\nERROR: No Tag Name label ("Assignment") row found. Unable to process strain data.')
        quit()

    # Get the units (Reduction Method)
    # Look for the label in the left column. If it exists, the resulting series
    # will have a True value in the position corresponding to the row.
    # If found, get the row, removing the first two columns.
    markLabel = 'Reduction Method:'
    markRows= df_source[idCol].str.match(markLabel, case=False, na=False)
    if markRows.any():
        # The label was found -- the row will be marked as true, which is considered
        # greater than false, so it will be the index with the max value.
        # Get the data frame row for this location, stripping off the first
        # two columns which contain the label and a blank location corresponding
        # to the elapsed time column.
        units= df_source.iloc[markRows.idxmax()][idCol + 2:]
        # Combine the tag from above, with the units so we don't need two rows
        # to display both -- TagName (units)
        # Tag name and units should be the same length, but just in case use min len
        for colNum in range(min(tagNames.size, units.size)):
            if units.iloc[colNum] and units.iloc[colNum].lower() != 'strain':
                # there is a unit specified, and it isn't strain. Concat with tag name
                tagNames.iloc[colNum] = tagNames.iloc[colNum] + ' (' + units.iloc[colNum] + ')'
            elif units.iloc[colNum] and units.iloc[colNum].lower() == 'strain':
                # there is a unit specified, but it is strain.
                # Concat 'uStrain' with tag name (add the u).
                tagNames.iloc[colNum] = tagNames.iloc[colNum] + ' (uStrain)'
    else:
        # Units label not found.  Gartz to go...
        print('\nERROR: No Units label ("Reduction Method") row found. Unable to process strain data.')
        quit()

    # Get elapsed times (time offsets)
    # Look for the label in the second column. If it exists, the resulting series
    # will have a True value in the position corresponding to the row.
    markLabel = 'Elapsed'
    # note use of contains since the mark is not at the beginning
    markRows = df_source[idCol + 1].str.contains(markLabel, case=False, na=False)
    if markRows.any():
        # The label was found.
        # Get the second column of the data frame row for this location to get
        # the elapsed time label.  This gives us access to time units, "seconds"
        # for example. Convert to lower case to avoid inconsistency from causing
        # searching errors.
        offsetLabel = df_source.iloc[markRows.idxmax()][idCol + 1].lower()
        # Get the elapsed time values in second column of data, and start
        # in the row following the row where the label was found.
        offsets= df_source[idCol + 1][markRows.idxmax() + 1:]
        # :TRICKY: Test for 'millisecond' first since it contains
        # the string 'seccond'. Labels will usually be plural, but leave the 's'
        # off to be less restrictive.
        if offsetLabel.find('milli') != -1:
            # time offset in units of milliseconds
            timeStamps = offsets.apply(lambda d: (startTime + pd.Timedelta(str(d) + 'milli')))
        elif offsetLabel.find('sec') != -1:
            # time offset in units of seconds
            timeStamps = offsets.apply(lambda d: (startTime + pd.Timedelta(str(d) + 'sec')))
        elif offsetLabel.find('min') != -1:
            # time offset in units of minutes
            timeStamps = offsets.apply(lambda d: (startTime + pd.Timedelta(str(d) + 'min')))
        elif offsetLabel.find('hour') != -1:
            # time offset in units of hours
            timeStamps = offsets.apply(lambda d: (startTime + pd.Timedelta(str(d) + 'hr')))
        else:
            # unknown offset units. Can't create timestamps. Print a message and go
            print('Elapsed time units could not be determined. Unalbe to process strain data.')
            quit()
    else:
        # Units label not found.  Gartz to go...
        print('\nERROR: No Elapsed Time label ("xxx Elapsed") column found. Unable to process strain data.')
        quit()

    # At this point, we processed the header, and now need to remove everything 
    # but the data, add the timestamps as an index, and a single header row using
    # the tagNames as the header.
    # drop the rows above the data (the header -- it isn't needed anymore)
    df_source.drop(df_source.index[:idRow + 1], axis=0, inplace=True)
    # now drop the two left most columns, the ID and the elapsed time
    df_source.drop(df_source.columns[:2], axis=1, inplace=True)
    # now rename the columns using the tag names.
    df_source.rename(columns=tagNames, inplace=True)
    # now add the timestamps to the dataframe
    df_source['timestamp'] = timeStamps
    # finally, set the timestamp column to be the index
    df_source.set_index('timestamp', inplace=True)
    print(df_source)
    quit()
    # In the normalized time data case, the first column is the timestamp, and
    # every column after the 3rd is instrument data headered with the instrument
    # name.
    print('\nNormalized Time Data Specified. The source data is expected to \
have the following format:\n\
    TimeStamp, Time Bias, Tag1 Value, Tag2 Value, ...\n')
    # TODO: Time Bias support
    # KLUDGE: Drop the time bias as the times are already in local time, which
    # is what is desired. May be better to do something smarter with it, like
    # modify the TsIdxData object to be UTC and timezone aware.
    # Timestamps are in local time. No need for the bias column. Drop it.
    # NOTE: This is needed in the merge sections also!!
    df_source.drop(columns=[headerList[1]], inplace=True, errors='ignore')

    # Deal with duplicates in input file.
    # Duplicates with this data format within the same file are problematic
    # because they represent a tag with more than one value at the same timestamp.
    # While this could be delt with when merging, it is an indication that the
    # data may not be as expected. Error out so a person needs to take a look.
    dups = listDuplicates(df_source)
    if dups:
        # duplicates have been found.  Notify leave.
        print('    ERROR: There are column names duplicated in the input file "' + args.inputFileName + '".\n\
This is not allowed with this type of data because it usually means ambiguous tag values.\n\
There will be no further processing.\nThe following column names are duplicated:')
        print(dups)
        quit()

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
        except ValueError as ve:
            print('    WARNING: Problem converting some timestamps from \
the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
            print(ve)
            df_source[tsName] = pd.to_datetime(df_source[tsName],
                                            errors='coerce',
                                            box = True,
                                            infer_datetime_format = True,
                                            origin = 'unix')
    # Remove any NaN/NaT values as a result of conversion
    df_source.dropna(subset=[tsName], how='any', inplace=True)
    # Rround the timestamp to the nearest ms. Unseen ns and
    # fractional ms values are not always displayed, and can cause
    # unexpected merge and up/downsample results.
    try:
        df_source[tsName] = df_source[tsName].dt.round('L')
    except ValueError as ve:
        print('    WARNING: Timestamp cannot be rounded.')
        print(ve)

    # Get rid of any duplicate timestamps. Done after rounding in case rounding
    # introduced dups.
    df_source.drop_duplicates(subset=[tsName], keep='last', inplace=True)
    # set the timestamp column to be the index
    df_source.set_index(tsName, inplace=True)
    # sort the index for possible better performance later
    df_source.sort_index(inplace=True)

    # If there are files specified to merge, merge them with the input file before
    # further processing. This file has times and tags that may match or
    # may be additional to the source data. The data merge will make the source
    # data wider if there are new tags, and longer if there are new times. One or
    # both may happen.
    # Merge File 1
    if args.archiveMerge1 is not None:
        try:
            print('Merging file "' + args.archiveMerge1 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge1, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am1/archiveMerge1 \
parameter: "' + args.archiveMerge1 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()

        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge1 + '" specified \
with the -am1/archiveMerge1 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge1 + '" specified with the -am1/archiveMerge1 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
            print('Merging file "' + args.archiveMerge2 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge2, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am2/archiveMerge2\
parameter: "' + args.archiveMerge2 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge2 + '" specified \
with the -am2/archiveMerge2 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge2 + '" specified with the -am2/archiveMerge2 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
            print('Merging file "' + args.archiveMerge3 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each 
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for 
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use 
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge3, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am3/archiveMerge3 \
parameter: "' + args.archiveMerge3 + '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge3 + '" specified \
with the -am3/archiveMerge3 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n\
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge3 + '" specified with the -am3/archiveMerge3 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
            print('Merging file "' + args.archiveMerge4 + '".\n')
            # use string as the data type for all columns to prevent automatic
            # datatype detection. We don't know ahead of time how many columns are
            # being read in, so we don't yet know the types.
            # We want duplicate column names to be preserved as in.
            # They will get filtered out as duplicates later.
            # The default behavior of read_csv is to append a ".n" to the column name
            # where n is an integer value starting at 1 and incrementing up for each
            # duplicate found. The problem with this is later, this gets interpreted as
            # a different tag if using some options.
            # COMPILER: mangle_dupe_cols=False is supposed to preserve the duplicate
            # column names by turning off the mangling described above.
            # It is "not supported yet" but is in the documentation for
            # Pandas 0.22 and maybe earler as being a feature!!
            # It throws a ValueError if used.  As a work around, don't use
            # mangle_dupe_cols=False, and use header=None instead of header=0 in the
            # read_csv function.  Then manually rename the columns using the 1st row
            # of the csv.
            df_merge = pd.read_csv(args.archiveMerge4, sep=args.sourceDelimiter,
                                delim_whitespace=False, encoding=args.sourceEncoding,
                                header=None, dtype = str, skipinitialspace=True)
                                # mangle_dupe_cols=False)
            df_merge = df_merge.rename(columns=df_merge.iloc[0], copy=False).iloc[1:].reset_index(drop=True)

        except ValueError as ve:
            print('ERROR opening the file specified with the -am4/archiveMerge4 \
parameter: "' + args.archiveMerge4+ '".\n Check file name, file presence, and permissions.  \
Unexpected encoding can also cause this error.')
            print(ve)
            quit()
        # Drop the time bias (second) column
        df_merge.drop(columns=[df_merge.columns[1]], inplace=True, errors='ignore')

        # Deal with duplicates in the merge file.
        # Duplicates with this data format within the same file are problematic
        # because they represent a tag with more than one value at the same timestamp.
        # While this could be delt with when merging, it is an indication that the
        # data may not be as expected. Error out so a person needs to take a look.
        dups = listDuplicates(df_merge)
        if dups:
            # duplicates have been found.  Notify leave.
            print('    ERROR: There are column names duplicated in the file "' + args.archiveMerge4 + '" specified \
with the -am4/archiveMerge4 parameter.\nThis is not allowed with this type of data because it usually \
means tag values would be ambiguous at a given time.\n \
There will be no further processing.\nThe following column names are duplicated:')
            print(dups)
            quit()

        # Deal with duplicates between the source and merge file.
        # Delay this until further below, once the merge data has been indexed by
        # timestamp. In other words, compare the columns for dups, not the timestamp
        # column name.

        # Index the merge data time stamp column. Since we know this will be the
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
            except ValueError as ve:
                print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                print(ve)
                df_merge[tsName] = pd.to_datetime(df_merge[tsName],
                                                errors='coerce',
                                                box = True,
                                                infer_datetime_format = True,
                                                origin = 'unix')
        # Remove any NaN/NaT values as a result of conversion
        df_merge.dropna(subset=[tsName], how='any', inplace=True)
        # Rround the timestamp to the nearest ms. Unseen ns and
        # fractional ms values are not always displayed, and can cause
        # unexpected merge and up/downsample results.
        try:
            df_merge[tsName] = df_merge[tsName].dt.round('L')
        except ValueError as ve:
            print('    WARNING: Timestamp cannot be rounded.')
            print(ve)

        # Get rid of any duplicate timestamps. Done after rounding in case rounding
        # introduced dups.
        df_merge.drop_duplicates(subset=[tsName], keep='last', inplace=True)
        # set the timestamp column to be the index
        df_merge.set_index(tsName, inplace=True)
        # sort the index for possible better performance later
        df_merge.sort_index(inplace=True)

        # Now that the source and merge data have both been indexed by timestamp,
        # we can deal with duplicate value column names between the source and
        # merge file.
        # With this data format, this may or may not be problematic. If there
        # are duplicated timestamps, they will get removed after merging.
        # Detect duplicates and warn.
        dups = listToListIntersection(df_source.columns.values, df_merge.columns.values)
        if dups:
            # duplicates have been found.  Notify and continue.
            print('    WARNING: There are tags in the input file "' + args.inputFileName + '" that are duplicated\n\
in the file "' + args.archiveMerge4 + '" specified with the -am4/archiveMerge4 parameter.\nThis is allowed, \
but if the duplicate column or columns contain duplicate timestamps, then only \n\
one value will be retained.\nThe following tags are duplicated:')
            print(dups)
            print()

        # Now merge the data. Append rows (axis=0), which actually is appending
        # to the index. Note that NaN values may result depending on which times
        # and values are being merged, but these will get removed later.
        df_merged = pd.concat([df_source, df_merge], axis=0, sort=False)

        # print diagnostic info if verbose is set
        if args.verbose:
            print('**** Merge Data ****')
            print(df_merge)
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
    # and the header contains the tag names for the value columns.

    # Update the header list after the merge to make sure new tags are reflected.
    headerList = df_source.columns.values.tolist()

    # Make sure the data is still sorted by time after the merge. This may be
    # unnecessary, but just in case.
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
        # Create a new instrument object and use the above column names.
        tid_inst = TsIdxData(instName, tsName, valName,
                             df_source.iloc[:,idx],
                             args.valueQuery, startArg, endArg,
                             sourceTimeFormat, forceColNames=True)
        # See if instrument is already in the list. If so append the
        # data to an existing instrument object already in the object list.
        # If not, then append a new object with the new data to the name and
        # object lists.
        if instName in instDataNames:
            # An instrument with the same name already exists.
            # Append this data to it
            idx = instDataNames.index(instName)
            print('Inst in list at index ' + str(idx) + '. Appending data.')

            # Appending the data will apply previously specified value queries
            # and time filtering
            instData[idx].appendData(tid_inst.data, 0) # don't ignore any rows
        else:
            # This instrument is not in the instrument list yet.
            # Append it to the name list and the object list
            print('Inst not yet seen. Appending new instrument to list of instruments.')
            instDataNames.append(instName)
            # Make an object with the instrument name, labels and data frame
            # instrument data object, and append it to the list.
            # Querying of value and filtering of timestamps will happen during
            # construction of the object
            instData.append(tid_inst)

        # The instrument data is now contained in the instrument InstData object.
        # Delete the instrument object to free up resources.
        del tid_inst

    # The data is now in instData in data frames. Done with the source data. Delete it.
    del df_source

# As long as there is an instrument list,
# sort the instrument list by instrument name.
# This is done here, so just the list is mutated,
# and possibly large datasets aren't being changed.
# Sort based on the last name version of the names, so the
# sort order is case insensitive.
# Do this here so it can be printed this way in verbose mode.
if instData:
    instData.sort(key=lambda x: x.name.lower())

# Print diagnostic info if verbose is set
if args.verbose:
    print('**** List of Instruments ****')
    print(instData)
    print()

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
    except ValueError as ve:
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
        except ValueError as ve:
            print('ERROR opening the output file. Nothing written.')
            quit()

        # generate the export compliance warning, unless explicitly omitted
        if not args.noExportMsg:
            expCompWarn = \
['WARNING - This document contains technical data export of which',
'is restricted by the Export Administration Regulations (EAR).',
'Release of this document is only authorized for the use of the',
'ITER Organization and its duly ratified member nations and their technical',
'representatives for the development of fusion energy for peaceful purposes.',
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
        except ValueError as ve:
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
