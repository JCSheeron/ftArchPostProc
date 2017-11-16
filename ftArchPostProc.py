#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Final Test Archive Data Post Processing
# This program accepts an input csv file, post processes it, and creates a csv
# output file.
#
# In the case of a historical trend generated file (the -t command line
# argument), the data columns are as follows:
# Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
# and the timestamps are not synchronized.
#
# In the case of a archive export file (the -a command line argument), the data
# columns are as follows:
# ValueId,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),value,quality,flags
# and there are normally multiple valueIDs each at multiple timestamps.
# Note: The -h and -a options are mutually exclusive, and one or the other must
# be specified. 
#
# Given an input file, the program will produce a *.csv file with the name
# specified as the outputFileName with the format:
# Timestamp, Tag1 Value, Tag2 Value ...
# TODO: More detail about what happens to combine and normalize timestamps
#
# The default field delimiter is the comma (","). If another delimiter needs to
# be specified, it can be done so using the -d (delimiter) option. If more than
# one character is specified, the delimiter will be interpreted as a regular
# expression.
#
# It is assumed that the first row is a header. If there is no header row, then
# use the -noheader command line option
# 
#
# Command line paraemters are:
# inputFileName (required, positional). The source data csv file.
#
# outputFileName (required, positional). The .csv output file name.
#
# -t, (required and mutually exclusive with -a).  Input file
# is a historical trend export file.
#
# -a (required and mutually exclusive with -h). Input file is a
# archive export file.  
#
#-d or --delimiter (optional, default of "'"). Delimiter. Single character or regex.
#
# -noheader (optional) Do not treat the first row as header info.

# imports

from datetime import datetime, time
from dateutil import parser as duparser

# import arg parser
import argparse

# import numerical manipulation libraries
import numpy as np
import pandas as pd

# create a TimeStamp Indexed data class
# TODO: Put this def in a module and import it
class TsIdxData(object):
    def __init__(self, name, tsName=None, yName=None, df=None,
            valueQuery=None, startQuery=None, endQuery=None):
        self._name = str(name) # use the string version

        # default x-axis (timestamp) label to 'timestamp' if nothing is specified
        if tsName is None:
            self._tsName = 'timestamp'
        else:
           self._tsName = str(tsName) # use the string version

        # default the y-axis label to the name if nothing is specified
        if yName is None:
            self._yName = name
        else:
           self._yName = str(yName) # use the string version

        # Keep the column (header) names as a property
        self._columns = [self._tsName, self._yName]

        # Default the filter sentinel to empty if not specified. 
        if valueQuery is None:
            self._vq = ''
        else:
            # something specified for the value  query string (vq)
            # make sure it is a string, and convert to lower case
            self._vq = str(valueQuery).lower()

        # Convert the start and end times to datetimes if they are specified.
        # Use the dateutil.parser function to get input flexability, and then
        # convert to a pandas datetime for max compatibility
        # If time info is not included in the start time, it defaults to
        # midnight, so comparisons will work as expected and capture the entire
        # day. For the end time, however, if time info is not included, force
        # it to be 11:59:59.999 so the entire end date is captured.
        if startQuery is None:
            self._startQuery = None
        else:
            # see if it is already a datetime. If it is, no need to do
            # anything. If it isn't then convert it. If there is a conversion
            # error, set to none and print a message
            if not isinstance(startQuery, pd.datetime):
                # need to convert
                try:
                    self._startQuery = duparser.parse(startQuery, fuzzy=True)
                    # convert to a pandas datetime for max compatibility
                    self._startQuery = pd.to_datetime(self._startQuery,
                                            errors='coerce',
                                            box=True,
                                            infer_datetime_format=True,
                                            origin='unix')
                except:
                    # not convertable ... invalid ... ignore
                    print('Invalid start query. Ignoring.')
                    self._startQuery = None
            else:
                # no need to convert
                self._startQuery = startQuery

        # repeat for end query
        if endQuery is None:
            self._endQuery = None
        else:
            # see if it is already a datetime. If it is, no need to do
            # anything. If it isn't then convert it. If there is a conversion
            # error, set to none and print a message
            if not isinstance(endQuery, pd.datetime):
                # need to convert
                try:
                    self._endQuery = duparser.parse(endQuery, fuzzy=True)
                    # assume the end time of midnight means end time info was not
                    # specified. Force it to the end of the day
                    if self._endQuery.time() == time(0,0,0,0):
                        self._endQuery = self._endQuery.replace(hour=23, minute=59, 
                                                  second=59, microsecond=999999)
                    # convert to a pandas datetime for max compatibility
                    self._endQuery = pd.to_datetime(self._endQuery, errors='coerce',
                                            box=True,
                                            infer_datetime_format=True,
                                            origin='unix')
                except:
                    # not convertable ... invalid ... ignore
                    print('Invalid end query. Ignoring.')
                    self._endQuery = None
            else:
                # no need to convert
                self._endQuery = endQuery

        if df is None:
            # create an empty data frame with the column names
            self._df = pd.DataFrame(columns=[self._tsName, self._yName])
            # force the columns to have the data types of datetime and float
            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                    errors='coerce')
            self._df[self._yName] = self._df[self._yName].astype('float',
                                                    errors='ignore')
            # set the timestamp as the index
            self._df.set_index(self._tsName, inplace=True)

            # **** statistics -- set to 0
            # Set the start and end timestamps to something not likely
            self._startTs = pd.NaT
            self._endTs = pd.NaT

            # clear the count, min, max, median, mean
            # median values
            self._count = 0
            self._min = 0
            self._max = 0
            self._median = 0
            self._mean = 0
        else:
            # set the data frame with the specified data frame
            self._df = pd.DataFrame(data=df)
            self._df.columns=[self._tsName, self._yName]
            # force the values to floats
            self._df[self._yName] = self._df[self._yName].astype('float',
                                                    errors='ignore')
            # get rid of Nan from the values (y-axis)
            # not strictly necessary, but lack of NaN values tends to make
            # follow on data analysis less problematic
            self._df.dropna(subset=[self._yName], inplace=True)
            # force the timestamp to be a datetime
            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                errors='coerce',
                                                box = True, 
                                                format = "%-m/%-d/%Y %H:%M:$S",
                                                infer_datetime_format = True,
                                                origin = 'unix')
            # get rid of any NaN and NaT timestamps. These can be from the
            # original data or from invalid conversions to datetime
            self._df.dropna(subset=[self._tsName], inplace=True)

            # Apply the query string if one is specified.
            # Replace "val" with the column name.
            if self._vq != '':
                queryStr = self._vq.replace("val", self._yName)
                # try to run the query string, but ignore it on error
                try:
                    self._df.query(queryStr, inplace = True)
                except:
                    print('Invalid query string. Ignoring the specified query.')

            # Make sure the data is sorted by timestamp. Even if the data seems
            # sorted, this is sometimes needed or the merge will create a
            # bunch of unexpected (erronous) NaN values.
            self._df.sort_values(self._tsName, ascending=True, inplace=True)
            # set the timestamp as the index
            self._df.set_index(self._tsName, inplace=True)

            # now the timestamp is the index, so filter based on the specified
            # start and end times
            self._df= self._df.loc[self._startQuery : self._endQuery]

            # **** statistics
            # get the start and end timestamps
            self._startTs = self._df.index.min()
            self._endTs = self._df.index.max()

            # get the count, min, max, mean, median values
            self._count = self._df[self._yName].count()
            self._min = self._df[self._yName].min()
            self._max = self._df[self._yName].max()
            self._median = self._df[self._yName].median()
            self._mean = self._df[self._yName].mean()

    def __repr__(self):
        colList= list(self._df.columns.values)
        outputMsg=  '{:8} {}'.format('Name: ', self._name + '\n')
        outputMsg+= '{:8} {:18} {:10} {}'.format('Index: ', self._df.index.name, \
'datatype: ', str(self._df.index.dtype) + '\n')
        outputMsg+= '{:8} {:18} {:10} {}'.format('Y axis: ', str(colList[0]), \
'datatype: ', str(self._df[colList[0]].dtype) + '\n')
        outputMsg+= '{:14} {}'.format('Query String: ', self._qs + '\n')
        outputMsg+= '{:14} {}'.format('Start Time: ', str(self._startTs) + '\n')
        outputMsg+= '{:14} {}'.format('End Time: ', str(self._endTs) + '\n')
        outputMsg+= '{:14} {}'.format('Value Count: ', str(self._count) + '\n')
        outputMsg+= '{:14} {}'.format('Min Value: ', str(self._min) + '\n')
        outputMsg+= '{:14} {}'.format('Max Value: ', str(self._max) + '\n')
        outputMsg+= '{:14} {}'.format('Median Value: ', str(self._median) + '\n')
        outputMsg+= '{:14} {}'.format('Mean Value: ', str(self._mean) + '\n')
        outputMsg+= str(self._df) + '\n'
        return(outputMsg)

    # read only properties
    @property
    def name(self):
        return self._name

    @property
    def tsName(self):
        return self._tsName
           
    @property
    def yName(self):
        return self._yName

    @property
    def queryString(self):
        return self._qs

    @property
    def columns(self):
        return self._columns

    @property
    def startTs(self):
        return self._startTs

    @property
    def endTs(self):
        return self._endTs

    @property
    def count(self):
        return self._count

    @property
    def min(self):
        return self._min

    @property
    def max(self):
        return self._max

    @property
    def median(self):
        return self._median

    @property
    def mean(self):
        return self._mean


print('*** Begin Processing ***')
# get start processing time
procStart = datetime.now()
print('Process start time: ' + procStart.strftime('%m/%d/%Y %H:%M:%S') + '\n')

# **** argument parsing
# define the arguments
# create an epilog string to further describe the input file
eplStr="""This program accepts an input csv file, post processes it, and
creates a csv output file. The format of the input file is given using the 
-t (historical trend export file) or the -a (archive export) option.

Note: -a OR -t must be specified, but they are mutually exclusive.

The format of the input file when the -t option is used is:
Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
and the timestamps are not synchronized.  There are normally multiple tags and 
multiple timestamps.

The format of the input file when the -a option is used is:
ValueId,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),value,quality,flags
and there are normally multiple valueIDs with multiple timestamps.

The default field delimiter is a comma (\",\"). If a different delimiter is
used for the source or destination file(s), it can be specified with the
-sd or --sourcedelimiter, or -dd or -destdelimiter options.

The first row is assumed to be header data (names).

The data can be filtered if a query string is specified.  The specified string
is used as a query string when the data is populated. The query string is
specified by the -vq or --valueQuery option.  "val" represents process values.

As an alternative to a query string, a start and/or end timestamp can be
specified using the -st or --starttime and -et or --endtime options.
The data will be merged over this time period.  If they are not specified, the
start and end times are derived from the data.

File encoding for the source or destination files  can be specified with the
-se or --sourceencoding, or -de or --destencoding options.  Default
encoding is utf_16.\n """

descrStr="Post Processing of historical trend or archive data files."
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
                                 description=descrStr,
                                 epilog=eplStr)
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
                    help='Specify a start time. Use the data if not specified.')
parser.add_argument('-et', '--endTime', default=None, metavar='', \
                    help='Specify an end time. Use the data if not specified.')
parser.add_argument('-rs', '--resample', default='S', metavar='', \
                    help='Resample. If period longer than 1 Sec is specified, \
then one value is used to represent many values.  A min, max, and average \
value is generated.  Default is 1S, and no min/max/avg is calculated.\
Options are (D)ay, (H)our, minu(T)e, (S)econd, mi(L)liseconds. \
You can put an integer in front of the option to further specify a period. \
For example, "5S" would be a 5 second sample period.')
# add -t and -a as a required, but mutually exclusive group
typegroup = parser.add_mutually_exclusive_group(required=True)
typegroup.add_argument('-t',  action='store_true', default=False, \
                    help='Historical trend input file type (format).')
typegroup.add_argument('-a', action='store_true', default=False, \
                    help='Archive data input file type (format).')
# parse the arguments
args = parser.parse_args()

# At this point, the arguments will be:
# Argument          Values      Description
# args.inputFileName    string  file to get data from
# args.outputFileName   string  file to write processed data to
# args.sourceDelimiter  string  Input file field delimiter. Default is (",")
# args.sourceEncoding   string  Input file encoding. Default is utf_16.
# args.destDelimiter    string  Dest file field delimiter. Default is (",")
# args.destEncoding     string  Dest file encoding. Default is utf_16.
# args.valueQuery       string  Optional query of the data
# args.startTime        string  Optional start date time
# args.endTime          string  Options end date time
# args.resample         string  Resample period. Default is 'S' or 1 sample/sec.
# args.t                True/False  Historical trend input file type when set
# args.a                True/False  Archive data input file type when set

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
        startArg = pd.to_datetime(startArg, errors='coerce', box=True,
                                  infer_datetime_format=True, origin='unix')
    except:
        # not convertable ... invalid ... ignore
        print('Invalid start time. Ignoring.')
        startArg = None
else:
    # arg is none, so update the internal version
    startArg = None

# repeat for end time
if args.endTime is not None:
    # Convert the argument to a datetime. If it can't be converted, ignore it.
    # need to convert
    try:
        endArg = duparser.parse(args.endTime, fuzzy=True)
        # convert to a pandas datetime for max compatibility
        endArg = pd.to_datetime(endArg, errors='coerce', box=True,
                                  infer_datetime_format=True, origin='unix')
    except:
        # not convertable ... invalid ... ignore
        print('Invalid end time. Ignoring.')
        endArg = None
else:
    # arg is none, so update the internal version
    endArg = None

# **** Read the csv file into a data frame.  The first row is treated as the header
df_source = pd.read_csv(args.inputFileName, sep=args.sourceDelimiter,
                    delim_whitespace=False, encoding=args.sourceEncoding,
                    header=0, skipinitialspace=True)
# put source the headers into a list
headerList = df_source.columns.values.tolist()
# make a spot for a list of instrument InstData objects
instData = []

# ****Iterate thru the header list.
# Create desired column names: value_<instName> and timestamp_<instName>
# Create a instrument data object with data sliced from the big data frame
# look at the -t or -a argument to know what format the data is in 
if args.t and len(headerList) >= 2:
    # historical trend data, and there are at least two (time/value pair) cols
    # In the historical trend case, loop thru every other column to get to the 
    # time stamp columns. The instrument name can be derrived from this and the 
    # values can be obtained from a relative (+1) index from the timestamp
    for idx in range(0, len(headerList), 2):
        # For each header entry, make instrument and timestamp column names.
        # Even indexes are timestamps, odd indexes are values.
        # get the inst name, leaving off the bit after the last space, which is
        # normally 'Time' or 'ValueY'
        # rpartition returns a tuple: first, separator, last. Use the first 
        # member as the tag name -- this allows tag names with spaces to be
        # preserved
        instName = headerList[idx].rpartition(' ')[0] 
        # replace the spaces and hyphens with underscores
        instName = instName.replace(' ', '_')
        instName = instName.replace('-', '_')
        # print a message showing what we are processing
        print('Processing ' + instName)
        # generate timestamp and value field (column) names
        # include the instr name in the timestamp column label so it can be
        # identified standalone
        tsName = 'timestamp_' + instName
        valName = 'value_' + instName
        # create a new dataframe for the instrument
        iDframe = pd.DataFrame(df_source.iloc[:,[idx,idx+1]]) 
        # make an object with the instrument name, labels and data frame
        # instrument data object, and append it to the list.
        # Querying of value and filtering of timestamps will happen during
        # construction of the object
        instData.append(TsIdxData(instName, tsName, valName, iDframe,
            args.valueQuery, startArg, endArg))

elif args.a and len(headerList) >= 2:
    # archive data, and there are at least two (time/value pair) cols
    # TODO: archive data case
    pass

# **** Determine the first start and end times for the instruments
# As long as there is a list of instrument objects,
# loop thru the instruments and get the first and last datetime
# init the start and end times
startTime= pd.NaT
endTime= pd.NaT
if instData:
    # find the earliest and latest start/end times
    for inst in instData:
        if not inst._df.empty and not pd.isna(inst.startTs) and pd.isna(startTime):
            # first valid time
            startTime = inst.startTs
        elif not inst._df.empty and not pd.isna(inst.startTs) and not pd.isna(startTime):
            # get min 
            startTime = min(startTime, inst.startTs)

        if not inst._df.empty and not pd.isna(inst.endTs) and pd.isna(endTime):
            # first valid time
            endTime = inst.endTs
        elif not inst._df.empty and not pd.isna(inst.endTs) and not pd.isna(endTime):
            # get the max
            endTime = max(endTime, inst.endTs)
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

    # **** Create a daterange data frame to act as the master datetime range.
    # The data will get left merged using this data frame for time
    # create the timestamp column name
    ts_name = 'timestamp'
    # using the start and end times, build an empty  dataframe with the 
    # date time range as the index
    try:
        df_dateRange = pd.DataFrame({ts_name:pd.date_range(startTime,
                                                           endTime,
                                                           freq=str(args.resample))})
    except:
        print('Error: Problem with generated date/time range. Check resample \
argument. Using a period of 1 sec')
        df_dateRange = pd.DataFrame({ts_name:pd.date_range(startTime, endTime, freq='S')})
    print(df_dateRange)
    # Make sure the date range is sorted. This is needed for the
    # merge to work as expected.
    df_dateRange.sort_values(ts_name, ascending=True, inplace=True)
    # set the timestamp as the index
    df_dateRange.set_index(ts_name, inplace=True)

    # **** Populate the destination data frame
    # As long as there is a list of instrument objects,
    # loop thru the instruments and merge the data into the destination data frame
    if instData:
        df_dest = df_dateRange
        for inst in instData:
            df_dest = pd.merge_asof(df_dest, inst._df,
                           left_index = True, right_index = True)
        # replace any NaN values in the resulting data frame with 0s so data users
        # are not tripped up with NaN
        df_dest.fillna(0.0, inplace = True)
        
        print(df_dest)
    print('Writing the output file')
    # **** Write the destination data frame to the output file
    #df_dest.to_csv(args.outputFileName, sep=args.destDelimiter,
    #        encoding=args.destEncoding)
else:
    print('No data found. Nothing written')

#get end  processing time
procEnd = datetime.now()
print('Process end time: ' + procEnd.strftime('%m/%d/%Y %H:%M:%S') + '\n')
print('Duration: ' + str(procEnd - procStart))
print('*** End Processing ***' + '\n')
