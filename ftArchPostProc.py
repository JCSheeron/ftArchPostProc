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
# It is assumed that the first row is a header. Tag names are derrived from the
# first row cell contents.
# 
#
# Command line arguments are:
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
# -se or --sourceEncoding (optional, default of "utf-16"). Source file encoding.
#
# -sd or --sourceDelimiter (optional, default of ","). Destination file field
# delimiter. Single character or regex.

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
# -st or --startTime (optional, default=None) Specify a start time.
# Use the earliest data time stamp if not specified.
#
# -et, or --endTime (optional, default=None) Specify an end time.
# Use the latest data time stamp if not specified.
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
# -stats' (optional, default='m') Choose which statistics to calculate when
# resampling. Ignored if not resampling (-rs must be specified for this option
# to do anything).  Choices are: (V)alue, m(I)n, ma(X), (a)verage/(m)ean,
# and (s)tandard deviation. Choices are not case sensitive. Default is 
# average/mean.  In the case of the Value option, the first value available
# which is on or after the timestamp is shown. The values between this and the
# next sample point are thrown away. For the other options, the intermediate
# values are used to calculate the statistic.
#
# TODO: Move Timestamp Indexed data class (TsIdxData) to a module
# TODO: Improved Error handling. Currently this is minimal

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
            valueQuery=None, startQuery=None, endQuery=None, resample=None,
            stats=None):
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

        # Get the string version of the resample argumnet
        if resample is None:
            self._resample = ''
        else:
            # get the string version
            self._resample = str(resample.upper())

        # Get the stats to calc. Use the passed in string or default to ave.
        if stats is None or stats == '':
            self._stats = 'm'
        else:
            self._stats = str(stats).lower()
        # determine the stat flags. Display the stat if the representative
        # character is in the stats argument. Find returns -1 if not found
        displayValStat = self._stats.find('v') > -1
        displayMinStat = self._stats.find('i') > -1
        displayMaxStat = self._stats.find('x') > -1
        displayMeanStat = self._stats.find('m') > -1 or self._stats.find('a') > -1
        displayStdStat = self._stats.find('s') > -1
        # If none of the flags are set, an invalid string must have been
        # passed. Display just the mean, and set the stats string accordingly
        if not displayValStat and not displayMinStat and \
                not displayMaxStat and not displayMeanStat and not displayStdStat:
            displayMeanStat = True
            self._stats = 'm'

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
            # No source specified ...
            # create different columns if resampling
            if self._resample == '':
                # not resampling ...
                # create an empty data frame with the column names
                self._df = pd.DataFrame(columns=[self._tsName, self._yName])
                # force the columns to have the data types of datetime and float
                self._df[self._yName] = self._df[self._yName].astype('float',
                                                        errors='ignore')
            else:
                # resample case
                # create an empty data frame with the column names
                # include statistical columns that are specified
                minColName = 'min_' + self._name
                maxColName = 'max_' + self._name
                meanColName = 'mean_'+ self._name
                stdColName = 'std_' + self._name
                self._df = pd.DataFrame(columns=[self._tsName])
                if displayValStat:
                    self._df[self._yName] = np.NaN
                    self._df = self._df.astype({self._yName: float}, errors = 'ignore')
                if displayMinStat:
                    self._df[minColName] = np.NaN
                    self._df = self._df.astype({minColName: float}, errors = 'ignore')
                if displayMaxStat:
                    self._df[maxColName] = np.NaN
                    self._df = self._df.astype({maxColName: float}, errors = 'ignore')
                if displayMeanStat:
                    self._df[meanColName] = np.NaN
                    self._df = self._df.astype({meanColName: float}, errors = 'ignore')
                if displayStdStat:
                    self._df[stdColName] = np.NaN
                    self._df = self._df.astype({stdColName: float}, errors = 'ignore')

            # force the timestamp to a datetime
            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                    errors='coerce')
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
            # Source data is specified ...
            # Capture the source data
            # set the data frame with the specified data frame
            self._df = pd.DataFrame(data=df)
            self._df.columns=[self._tsName, self._yName]
            # force the value column to a float
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

            # At this point we are done if we are not resampling.  If we are
            # resampling, then create a resampled dataframe, and use the whole
            # dataframe to resample. Once done, overwrite the dataframe with
            # the resampled one.
            if self._resample != '':
                # resample case
                # create an empty data frame with the column names
                # include statistical columns that are specified
                
                minColName = 'min_' + self._name
                maxColName = 'max_' + self._name
                meanColName = 'mean_'+ self._name
                stdColName = 'std_' + self._name
                self._dfResample = pd.DataFrame(columns=[self._tsName])
                if displayValStat:
                    self._dfResample[self._yName] = np.NaN
                    self._dfResample = self._dfResample.astype({self._yName: float}, errors = 'ignore')
                if displayMinStat:
                    self._dfResample[minColName] = np.NaN
                    self._dfResample = self._dfResample.astype({minColName: float}, errors = 'ignore')
                if displayMaxStat:
                    self._dfResample[maxColName] = np.NaN
                    self._dfResample = self._dfResample.astype({maxColName: float}, errors = 'ignore')
                if displayMeanStat:
                    self._dfResample[meanColName] = np.NaN
                    self._dfResample = self._dfResample.astype({meanColName: float}, errors = 'ignore')
                if displayStdStat:
                    self._dfResample[stdColName] = np.NaN
                    self._dfResample = self._dfResample.astype({stdColName: float}, errors = 'ignore')

                # force the timestamp to a datetime datatype
                self._dfResample[self._tsName] = \
                    pd.to_datetime(self._dfResample[self._tsName], errors='coerce')

                # set the timestamp as the index
                self._dfResample.set_index(self._tsName, inplace=True)

                # resample the data from the complete dataframe. Get the value
                # to resample using iloc so we don't need the column name.
                # Populate the statistics columns based on what statistics
                # were specified
                if displayValStat:
                    self._dfResample[self._yName] = \
                            self._df.iloc[:,0].resample(self._resample).first()

                if displayMinStat:
                    self._dfResample[minColName] = \
                            self._df.iloc[:,0].resample(self._resample).min()

                if displayMaxStat:
                    self._dfResample[maxColName] = \
                            self._df.iloc[:,0].resample(self._resample).max()

                if displayMeanStat:
                    self._dfResample[meanColName] = \
                            self._df.iloc[:,0].resample(self._resample).mean()

                if displayStdStat:
                    self._dfResample[stdColName] = \
                            self._df.iloc[:,0].resample(self._resample).std()
                
                # now overwrite the original dataframe with the resampled one
                # and delete the resampled one
                self._df = self._dfResample
                del self._dfResample

    def __repr__(self):
        colList= list(self._df.columns.values)
        outputMsg=  '{:8} {}'.format('Name: ', self._name + '\n')
        outputMsg+= '{:8} {:18} {:10} {}'.format('Index: ', self._df.index.name, \
'datatype: ', str(self._df.index.dtype) + '\n')
        outputMsg+= '{:8} {:18} {:10} {}'.format('Y axis: ', str(colList[0]), \
'datatype: ', str(self._df[colList[0]].dtype) + '\n')
        outputMsg+= '{:14} {}'.format('Value Query: ', self._vq + '\n')
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
eplStr="""Final Test Archive Data Post Processing
 This program accepts an input csv file, post processes it, and creates a csv
 output file.

 In the case of a historical trend generated file (the -t command line
 argument), the data columns are as follows:
 Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...
 and the timestamps are not synchronized.

 In the case of a archive export file (the -a command line argument), the data
 columns are as follows:
 ValueId,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),value,quality,flags
 and there are normally multiple valueIDs each at multiple timestamps.
 Note: The -h and -a options are mutually exclusive, and one or the other must
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

 It is assumed that the first row is a header. Tag names are derrived from the
 first row cell contents.
 

 Command line arguments are:
 inputFileName (required, positional). The source data csv file.

 outputFileName (required, positional). The .csv output file name.

 -t, (required and mutually exclusive with -a).  Input file
 is a historical trend export file.

 -a (required and mutually exclusive with -h). Input file is a
 archive export file.  

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

 -st or --startTime (optional, default=None) Specify a start time.
 Use the earliest data time stamp if not specified.

 -et, or --endTime (optional, default=None) Specify an end time.
 Use the latest data time stamp if not specified.

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
 and (s)tandard deviation. Choices are not case sensitive. Default is 
 average/mean.  In the case of the Value option, the first value available
 which is on or after the timestamp is shown. The values between this and the
 next sample point are thrown away. For the other options, the intermediate
 values are used to calculate the statistic."""


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
parser.add_argument('-rs', '--resample', default=None, metavar='', \
                    help='Resample the data. This is usually \
 used to "downsample" data. For example, create an output file with 1 sample \
 per minute when given an input file with 1 sample per second. If a period \
 longer than the source data sample period is specified, then one value is \
 used to represent more than one row in the source file.  In this case, the \
 -stats option is used (see below) to specify what statistices are calculated \
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
 and (s)tandard deviation. Choices are not case sensitive. Default is \
 average/mean.  In the case of the Value option, the first value available \
 which is on or after the timestamp is shown. The values between this and the \
 next sample point are thrown away. For the other options, the intermediate \
 values are used to calculate the statistic.')

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
# args.stats            string  Stats to calc. Value, min, max, ave, std dev.
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

# get the resample argument
if args.resample is not None:
    resample = str(args.resample) # use the string version
else:
    # arg is none, so default to 1S
    resample = None

# force the stats argument to a lower case string
stats = str(args.stats).lower()

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
            args.valueQuery, startArg, endArg, resample = resample,
            stats = stats))

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
    # date time range as the index. Default sample period to 1 Sec
    try:
        if resample is None:
            df_dateRange = pd.DataFrame({ts_name:pd.date_range(startTime,
                                                               endTime,
                                                               freq='S')})
        else:
            df_dateRange = pd.DataFrame({ts_name:pd.date_range(startTime,
                                                               endTime,
                                                               freq=resample)})
    except:
        print('Error: Problem with generated date/time range. Check resample \
argument.')
        quit()

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

    print('Writing the output file\n')
    #print(df_dest)
    # **** Write the destination data frame to the output file
    df_dest.to_csv(args.outputFileName, sep=args.destDelimiter,
            encoding=args.destEncoding)
else:
    print('No data found. Nothing written\n')

#get end  processing time
procEnd = datetime.now()
print('Process end time: ' + procEnd.strftime('%m/%d/%Y %H:%M:%S'))
print('Duration: ' + str(procEnd - procStart))
print('*** End Processing ***' + '\n')
