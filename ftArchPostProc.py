#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Final Test Archive Data Post Processing
# This program accepts an input csv file, post processes it, and creates a csv
# output file.  An export control message is included at the head of the output
# file, unless the -noExportMsg argument is used.
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
# TODO: Move Timestamp Indexed data class (TsIdxData) to a module
# TODO: Improved Error handling. Currently, error handling is minimal
# TODO: Decide how to handle CalcStats function:  What columns do we run stats
# on when the names change due to resampling (downsampling)? Force the value
# column always and always use this? Use the 1st (0th) column always? Something
# else?
# TODO: Print start and time messages to the screen.
# TODO: Not parsing dump data correctly. Problem wiht 10ms freq?
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
            # see if it is already a datetime. If it is, just update the member
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
                # no need to convert. Update the member
                self._endQuery = endQuery

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

        if df is None:
            # No source specified ...
            # create an empty data frame
            # not resampling ...
            # create an empty data frame with the column names
            self._df = pd.DataFrame(columns=[self._tsName, self._yName])
            # force the columns to have the data types of datetime and float
            self._df[self._yName] = self._df[self._yName].astype('float',
                                                    errors='ignore')

            # force the timestamp to a datetime
            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                    errors='coerce')
            # set the timestamp as the index
            self._df.set_index(self._tsName, inplace=True)

            # set the other properties
            self._timeOffset = np.NaN

            # **** statistics -- set to 0
            self.ClearStats()
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
                                                #format = "%m/%d/%Y %H:%M:$S.%f",
                                                #unit = 'ms',
                                                infer_datetime_format = True,
                                                origin = 'unix')
            # get rid of any NaN and NaT timestamps. These can be from the
            # original data or from invalid conversions to datetime
            self._df.dropna(subset=[self._tsName], inplace=True)

            # round the timestamp to the nearest ms. Unseen ns and
            # fractional ms values are not always displayed, and can cause
            # unexpected merge and up/downsample results
            self._df[self._tsName] = self._df[self._tsName].dt.round('L')

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

            # Get the inferred frequency of the index. Store this internally,
            # and expose below as a property
            try:
                inferFreq = pd.infer_freq(self._df.index)
                if inferFreq is not None:
                    self._timeOffset = to_offset(inferFreq)
                else:
                    self._timeOffset = None
            except:
                self._timeOffset = None

            # **** statistics
            self.CalcStats()

    def __repr__(self):
        colList= list(self._df.columns.values)
        outputMsg=  '{:8} {}'.format('Name: ', self._name + '\n')
        outputMsg+= '{:8} {:18} {:10} {}'.format('Index: ', self._df.index.name, \
'datatype: ', str(self._df.index.dtype) + '\n')
        outputMsg+= '{:8} {:18} {:10} {}'.format('Y axis: ', str(colList[0]), \
'datatype: ', str(self._df[colList[0]].dtype) + '\n')
        outputMsg+= '{:15} {}'.format('Value Query: ', self._vq + '\n')
        outputMsg+= '{:15} {}'.format('Start Time: ', str(self._startTs) + '\n')
        outputMsg+= '{:15} {}'.format('End Time: ', str(self._endTs) + '\n')
        outputMsg+= '{:15} {}'.format('Sample Period: ', str(self._timeOffset) + '\n')
        """
        outputMsg+= '{:15} {}'.format('Value Count: ', str(self._count) + '\n')
        outputMsg+= '{:15} {}'.format('Min Value: ', str(self._min) + '\n')
        outputMsg+= '{:15} {}'.format('Max Value: ', str(self._max) + '\n')
        outputMsg+= '{:15} {}'.format('Median Value: ', str(self._median) + '\n')
        outputMsg+= '{:15} {}'.format('Mean Value: ', str(self._mean) + '\n\n')
        """
        outputMsg+= str(self._df) + '\n'
        return(outputMsg)

    def CalcStats(self):
        # get the start and end timestamps
        self._startTs = self._df.index.min()
        self._endTs = self._df.index.max()
        """
        # get the count, min, max, mean, median values
        self._count = self._df[self._yName].count()
        self._min = self._df[self._yName].min()
        self._max = self._df[self._yName].max()
        self._median = self._df[self._yName].median()
        self._mean = self._df[self._yName].mean()
        self._stdDev = self._df[self._yName].std()
        """
        return

    def ClearStats(self):
        # Set the start and end timestamps to nothing
        self._startTs = pd.NaT
        self._endTs = pd.NaT

        # clear the count, min, max, median, mean
        self._count = 0
        self._min = 0
        self._max = 0
        self._median = 0
        self._mean = 0
        self._stdDev = 0
        return

    def resample(self, resampleArg='S', stats='m'):
        # Resample the data from the complete dataframe.
        # Determine if we are up or down sampling by comparing the
        # specified frequency (time offset) with the data frequency.
        # If the data is being upsampled (increase the frequency), than values
        # will be forward filled to populate gaps in the data.
        # If the data is being downsampled (decrease in frequency), then the
        # specified stats will be calculated on values that fall between those
        # being sampled.
        #
        # Make sure the resample argument is valid
        if resampleArg is None:
            # no sample period specified, use 1 second
            print(self._name + ': No resample period specified. Using 1 Second.')
            resampleTo = to_offset('S')
        else:
            try:
                resampleTo = to_offset(resampleArg)
            except:
                print(self._name + ': Invalid resample period specified. Using 1 second.')
                resampleTo = to_offset('S')

        if resampleTo < self.timeOffset:
            # Data will be upsampled. We'll have more rows than data.
            # Forward fill the data for the new rows -- a new row will use the
            # previous recorded value until a new recorded value is available.
            # In other words -- carry a value forward until a new one is avail.
            # The stats argument is ignored.
            #
            # Create a new data frame with a timestamp and value column, and 
            # force the data type to timestamp and float
            dfResample = pd.DataFrame(columns=[self._tsName])
            dfResample[self._yName] = np.NaN
            dfResample = dfResample.astype({self._yName: float}, errors = 'ignore')
            dfResample[self._tsName] = \
                pd.to_datetime(dfResample[self._tsName], errors='coerce')

            # set the timestamp as the index
            dfResample.set_index(self._tsName, inplace=True)
            # upsample the data
            try:
                dfResample[self._yName] = \
                        self._df.iloc[:,0].resample(resampleTo).pad()
                # print a message
                print(self.name + ': Upsampled from ' + str(self.timeOffset) + \
                     ' to ' + str(resampleTo))
                # update the object frequency
                self._timeOffset = resampleTo
                # now overwrite the original dataframe with the resampled one
                # and delete the resampled one
                self._df = dfResample
                del dfResample
                self.CalcStats()
                return
            except:
                print(self._name + ': Unable to resample data. Data \
unchanged. Frequency is ' + str(self.timeOffset))
                print('Error: ', sys.exc_info())
                return
        elif resampleTo > self.timeOffset:
            # Data will be downsampled. We'll have more data than rows.
            # This means we can calculate statistics on the values between
            # those being displayed.  Use the stats option to determine which
            # stats are to be calculated.

            # make stats not case sensitive
            if stats is not None:
                self._stats = str(stats).lower()

            # Determine column names.
            # Determine the stat flags. These are used below to decide which
            # columns to make and calculate. Display the stat if the representative
            # character is in the stats argument. Find returns -1 if not found
            displayValStat = stats.find('v') > -1
            displayMinStat = stats.find('i') > -1
            displayMaxStat = stats.find('x') > -1
            displayMeanStat = stats.find('m') > -1 or stats.find('a') > -1
            displayStdStat = stats.find('s') > -1
            # If none of the flags are set, an invalid string must have been
            # passed. Display just the mean, and set the stats string accordingly
            if not displayValStat and not displayMinStat and \
               not displayMaxStat and not displayMeanStat and \
               not displayStdStat:
                displayMeanStat = True
                stats = 'm'
                
            minColName = 'min_' + self._name
            maxColName = 'max_' + self._name
            meanColName = 'mean_'+ self._name
            stdColName = 'std_' + self._name

            # Create a new data frame with a timestamp and value column(s), and 
            # force the data type(s) to timestamp and float
            dfResample = pd.DataFrame(columns=[self._tsName])
            if displayValStat:
                dfResample[self._yName] = np.NaN
                dfResample = dfResample.astype({self._yName: float}, errors = 'ignore')
            if displayMinStat:
                dfResample[minColName] = np.NaN
                dfResample = dfResample.astype({minColName: float}, errors = 'ignore')
            if displayMaxStat:
                dfResample[maxColName] = np.NaN
                dfResample = dfResample.astype({maxColName: float}, errors = 'ignore')
            if displayMeanStat:
                dfResample[meanColName] = np.NaN
                dfResample = dfResample.astype({meanColName: float}, errors = 'ignore')
            if displayStdStat:
                dfResample[stdColName] = np.NaN
                dfResample = dfResample.astype({stdColName: float}, errors = 'ignore')

            # force the timestamp to a datetime datatype
            dfResample[self._tsName] = \
                pd.to_datetime(dfResample[self._tsName], errors='coerce')

            # set the timestamp as the index
            dfResample.set_index(self._tsName, inplace=True)

            # now do the resampling for each column
            # NOTE: fractional seconds can make merging appear to behave
            # strangely if precision gets truncated.
            try:
                if displayValStat:
                    dfResample[self._yName] = \
                            self._df.iloc[:,0].resample(resampleTo,
                            label='right', closed='right').last()

                if displayMinStat:
                    dfResample[minColName] = \
                            self._df.iloc[:,0].resample(resampleTo,
                            label='right', closed='right').min()

                if displayMaxStat:
                    dfResample[maxColName] = \
                            self._df.iloc[:,0].resample(resampleTo,
                            label='right', closed='right').max()

                if displayMeanStat:
                    dfResample[meanColName] = \
                            self._df.iloc[:,0].resample(resampleTo,
                            label='right', closed='right').mean()

                if displayStdStat:
                    dfResample[stdColName] = \
                            self._df.iloc[:,0].resample(resampleTo,
                            label='right', closed='right').std()
                # print a message
                print(self.name + ': Downsampled from ' + str(self.timeOffset) + \
                     ' to ' + str(resampleTo))
                # update the object frequency
                self._timeOffset = resampleTo
                # now overwrite the original dataframe with the resampled one
                # and delete the resampled one
                self._df = dfResample
                del dfResample
                self.CalcStats()
                return
            except:
                print(self._name + ': Unable to resample data. Data \
unchanged. Frequency is ' + str(self.timeOffset))
                print('Error: ', sys.exc_info())
                return
        else:
            # resampling not needed. Specified freq matches data already
            print(self.name + ': Resampling not needed. New frequency \
matches data frequency. Data unchanged. Frequency is ' + str(self.timeOffset))
            return
           
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
        return self._df.columns.values.tolist()

    @property
    def data(self):
        return self._df

    @property
    def timeOffset(self):
        return self._timeOffset

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

    @property
    def stdDev(self):
        return self._std


print('*** Begin Processing ***')
# get start processing time
procStart = datetime.now()
print('Process start time: ' + procStart.strftime('%m/%d/%Y %H:%M:%S') + '\n')

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
 values are used to calculate the statistic.

 -noExportMsg (optional, default=False). When this argument is used, it turns
 off the inclusion of an export control message.  The defaults to false, so a
 message is included unless this argument is specified. """

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

parser.add_argument('-noExportMsg', action='store_true', default=False, \
                    help='Do not include the export control message at the \
head of the output file when specified.')

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
# args.noExportMsg      True/False Exclude export control message when set
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
    try:
        endArg = duparser.parse(args.endTime, fuzzy=True)
        # convert to a pandas datetime for max compatibility
        endArg = pd.to_datetime(endArg, errors='coerce', box=True,
                                  infer_datetime_format=True, origin='unix')

        # assume the end time of midnight means end time info was not
        # specified. Force it to the end of the day
        if endArg.time() == time(0,0,0,0):
            endArg = endArg.replace(hour=23, minute=59, 
                                    second=59, microsecond=999999)

        # convert to a pandas datetime for max compatibility
        endArg = pd.to_datetime(endArg, errors='coerce',
                                box=True,
                                infer_datetime_format=True,
                                origin='unix')
    except:
        # not convertable ... invalid ... ignore
        print('Invalid end time. Ignoring.')
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
        print('Invalid resample period specified. Using 1 second')
        resampleArg = to_offset('S')
else:
    # arg is none, so update the internal version
    resampleArg = None

# force the stats argument to a lower case string so they are case insensitive.
stats = str(args.stats).lower()

# **** Read the csv file into a data frame.  The first row is treated as the header
try:
    df_source = pd.read_csv(args.inputFileName, sep=args.sourceDelimiter,
                        delim_whitespace=False, encoding=args.sourceEncoding,
                        header=0, skipinitialspace=True)
except:
    print('Error opening source file: "' + args.inputFileName + '". Check file \
name, file presence, and permissions.')
    quit()

# put source the headers into a list
headerList = df_source.columns.values.tolist()
# make a spot for a list of instrument InstData objects
instData = []

# ****Iterate thru the header list.
# Create desired column names: value_<instName> and timestamp_<instName>
# Create a instrument data object witpythonh data sliced from the big data frame
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
        if not inst._df.empty and not pd.isna(inst.startTs) and pd.isna(startTime):
            # first valid time
            startTime = inst.startTs
        elif not inst._df.empty and not pd.isna(inst.startTs) and not pd.isna(startTime):
            # get min 
            startTime = min(startTime, inst.startTs)

        # get the latest end time
        if not inst._df.empty and not pd.isna(inst.endTs) and pd.isna(endTime):
            # first valid time
            endTime = inst.endTs
        elif not inst._df.empty and not pd.isna(inst.endTs) and not pd.isna(endTime):
            # get the max
            endTime = max(endTime, inst.endTs)

        # get the highest frequency in the form of a time offset
        if not inst._df.empty and not pd.isna(inst.timeOffset) and pd.isna(freq):
            # first valid offset
            freq = inst.timeOffset
        elif not inst._df.empty and not pd.isna(inst.timeOffset) and not pd.isna(freq):
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
    # force the start time to start on a whole number of msec. Fractional
    # values can cause issues with merging and resampling.
    startTime = startTime.floor('L')
    # if resampling is going on, roll back to clean start point. This *should*
    # make the date range being merged with compatible with the resampled data
    # points -- resampling starts at the time offset "origin"
    if resampleArg is not None:
        print('ResampleArg:', resampleArg)
        print('Start before rollback:', str(startTime))
        #startTime = to_offset('T').rollback(startTime)
        startTime = startTime.floor(resampleArg)
        print('Start after rollback:', str(startTime))

    # **** Make sure the resampleArg is either the value specified or the
    # minimum of the instrument data frequencies if nothing was specified.
    if resampleArg is None:
        resampleArg = freq
        
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
        print('Error: Problem with generated date/time range. Check the \
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
            print('Error opening the output file. Nothing written.')
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
            print('Writing the output file\n')
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
            print('resampled instrument')
            print(inst._df)
            # Merge the instrument data with the master dataframe.
            # The forward direction means to take the first instrument value
            # that is on or after the master date range.
            # NOTE: Merge can appear to work strangely when fractional
            # msec are being used, and results are perhaps truncated or
            # rounded. Steps were taken during construction to round times to
            # the nearest msec.
            df_dest = pd.merge_asof(df_dest, inst._df,
                                    left_index = True, right_index = True,
                                    direction = 'backward')

        # replace any NaN values in the resulting data frame with 0s so data users
        # are not tripped up with NaN
        df_dest.fillna(0.0, inplace = True)

        #print(df_dest)
        #try:
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
        #except:
        #    print('\nError writing data to the file. Output file content is suspect.\n')
        #    print('Error: ', sys.exc_info())
        outFile.close()
    #       df_dest.to_csv(args.outputFileName, sep=args.destDelimiter,
    #            encoding=args.destEncoding)
    else:
        print('No instrument data found. Nothing written\n')

else:
    print('No data found. Nothing written\n')

#get end  processing time
procEnd = datetime.now()
print('Process end time: ' + procEnd.strftime('%m/%d/%Y %H:%M:%S'))
print('Duration: ' + str(procEnd - procStart))
print('*** End Processing ***' + '\n')
