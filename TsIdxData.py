#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# imports
#
# system related
import sys
# date and time stuff
from datetime import datetime, time
from pandas.tseries.frequencies import to_offset
from dateutil import parser as duparser

# umerical manipulation libraries
import numpy as np
import pandas as pd

# Class: TsIdxData
# File: TsIdxData.py
#
# Timestamped Indexed Data
#
# This file implements an data contaner which holds time stamped values,
# indexed and sorted by time stamp. The data container is implemented as a
# Pandas Dataframe.  The timestamp is a Datetime, and the values are floats. 
#
# The source data used to populate the container can have multiple value
# columns, but one column needs have a name which matches yName. This column
# will be considered the "value" column.  
#
# The source data used to populate the container also must have one value
# column or an index column with a name that matches tsName. This column will be
# used as the index.
#
# The constructore is expecting a data frame which is used as the source data.
#
# The constructor (ctor) has these areguments:
#   name -- The name to give the object. An instrument name for example.
#
#   tsName -- The name of the timestamp (index) column.
#
#   yName -- The name of the value column.
#
#   df -- The source data used to populate the data container. If a data frame
#         is not specified, then an empty data frame is created.  See
#         "Data Structure Notes" below.
#
#   valueQuery -- Query string used to filter the dataset.
#                 Default is empty, so nothing is filtered out. Use "val" to
#                 represent the process value(s). For example, to filter out
#                 all values < 0 or > 100, you want to keep everything else,
#                 so the filter string would be: "val >= 0 and val <= 100".
#
#   startQuery -- Datetime string used to filter the dataset.  Data timestamped
#                 before this time will be filtered out. The default is empty,
#                 so no data is filtered out if nothing is specified.
#
#   endQuery -- Datetime string used to filter the dataset.  Data timestamped
#               after this time will be filtered out. The default is empty,
#               so no data is filtered out if nothing is specified.
#
#   sourceTimeFormat -- Specify the time format of the source data timestamp.
#                       If nothing is specified, the value defaults to
#                       "%m/%d/%Y %I:%M:%S %p". A string using the following
#                       symbolic placeholders is used to specify the format: 
#                           %m minutes, %d days, %Y 4 digit year, 
#                           %y two digit year, %H hours (24hr format),
#                           %I hours (12 hr format), %M minutes, %S seconds,
#                           %f for fractional seconds (e.g. %S.%f), %p AM/PM.
#
# DATA STRUCTURE NOTES
#   The source data must have the following structure:
#       Timestamp data: An index or value column must exist
#                       labeled with the tsName specified.  This data must be of 
#                       type datetime or convertable to datetime. It will
#                       be converted, if needed, to a datetime the format
#                       specified with the sourceTimeFormat string.
#
#       Value data:     A value (non-index) column must exist labeled with the 
#                       yName specified. This data msut be of type float or
#                       or convertable to a float. It will be converted if needed.
#
#                       Other value (non-index) columns are allowed, the names 
#                       don't matter, as long as one of them is named yName
#                       per above.
#      
#
# In addition the data can be resampled.  Resampling makes the most sense when 
# the original data has time stamps at regular intervals, and the interval needs
# to be changed. If the data is being upsampled (increase the frequency),
# than values will be forward filled to populate gaps in the data. If the data
# is being downsampled (decrease in frequency), then the specified stats will
# be calculated on values that fall between those being sampled.
#
# When resampling, and data is being downsampled, stats can be calculated. The
# stats parameter is used to specify which stats to calculate.  It is optional
# and defaults to 'm' if not specified. Choices are: (V)alue, m(I)n, ma(X),
# (a)verage/(m)ean, and (s)tandard deviation.
# The (a) and (m) options do the same thing. Choices are not case sensitive.
# Default is average/mean (m).  In the case of the Value option,
# the first value available which is on or after the timestamp is shown.
# The values between this and the next sample point are thrown away.
# For the other options, the intermediate values are used to calculate the
# statistic.  Note: The stats parameter is ignored when upsampling.
#
# The following read only properties are implemented
#    name
#       string -- object name
#
#    tsName
#       timestamp -- column name
#
#    valueQuery
#       string used to query the source data during construction
#
#    columns
#       dictionary with column names as the key and the data type as a value {col name : datatype, ...}
#
#    data
#        a copy of the dataframe
#
#    timeOffset
#        time period between data samples
#
#     startTs
#         start time filter used to query the source data during construction
#    
#     endTs
#        end time filter used to query the source data during construction 
#
#     count
#        the number of rows in the data frame 
#
# TODO: There is no way to append or replace data.  The appendData and 
# replaceData methods need an implementation. Currently,
# if a TsIdxData object is created without specifing a source data frame (df param)
# then the object data will be an empty dataframe, and there is no way to populate
# it.  
#
class TsIdxData(object):
    def __init__(self, name, tsName=None, yName=None, df=None,
            valueQuery=None, startQuery=None, endQuery=None,
            sourceTimeFormat='%m/%d/%Y %I:%M:%S %p'):
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

        # Default the value query to empty if not specified. 
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
                                            #format='%m/%d/%Y %I:%M:%S %p',
                                            errors='raise',
                                            box=True,
                                            infer_datetime_format=True,
                                            origin='unix')
                except:
                    # not convertable ... invalid ... ignore
                    print('    WARNING: Invalid start query. Ignoring.')
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
                    # If and end time was not specified, force it to the end
                    # of the day so the entire date is included.
                    if self._endQuery.time() == time(0,0,0,0):
                        self._endQuery = self._endQuery.replace(hour=23, minute=59, 
                                                  second=59, microsecond=999999)

                    # convert to a pandas datetime for max compatibility
                    self._endQuery = pd.to_datetime(self._endQuery,
                                                    #format='%m/%d/%Y %I:%M:%S %p',
                                                    errors='raise',
                                                    box=True,
                                                    infer_datetime_format=True,
                                                    origin='unix')
                except:
                    # not convertable ... invalid ... ignore
                    print('    WARNING: Invalid end query. Ignoring.')
                    self._endQuery = None
            else:
                # no need to convert. Update the member
                self._endQuery = endQuery

                # If and end time was not specified, force it to the end
                # of the day so the entire date is included.
                if self._endQuery.time() == time(0,0,0,0):
                    self._endQuery = self._endQuery.replace(hour=23, minute=59, 
                                              second=59, microsecond=999999)

                # convert to a pandas datetime for max compatibility
                self._endQuery = pd.to_datetime(self._endQuery, errors='coerce',
                                        box=True,
                                        infer_datetime_format=True,
                                        origin='unix')

        # make sure the source time format is a string
        self._sourceTimeFormat = str(sourceTimeFormat)

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
            # should not raise an error, as there is no data 
            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                    errors='coerce')
            # set the timestamp as the index
            self._df.set_index(self._tsName, inplace=True)

            # set the other properties
            self._timeOffset = np.NaN
        else:
            # Source data is specified ...
            # It needs to have a value column named yName, and a value column
            # or index named tsName. See if this is true before proceeding,
            # and don't use data if not.
            srcCols = df.columns
            srcIndex = df.index.name
            if not (self._yName in srcCols) or not (self._tsName in srcCols or self._tsName == srcIndex):
                # source data column names are not as needed. Print a msg and bail
                print('TsIdxData constructor source data name problem for ' + self._name + '. \n \
The source data needs to have a value column named "' + self._yName + '" and a value \n \
or column or index named "' + self._tsName + '". This does not seem to be the case. \n \
The source data is unused, leaving the data set empty.\n ' )
                # create an empty data frame with the column names
                self._df = pd.DataFrame(columns=[self._tsName, self._yName])
                # force the columns to have the data types of datetime and float
                self._df[self._yName] = self._df[self._yName].astype('float',
                                                        errors='ignore')

                # force the timestamp to a datetime
                # should not raise an error, as there is no data 
                self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                        errors='coerce')
                # set the timestamp as the index
                self._df.set_index(self._tsName, inplace=True)

                # set the other properties
                self._timeOffset = np.NaN
            else:
                # The source data has the correct names. Continue.
                # Capture the source data
                self._df = pd.DataFrame(data=df)
                # change the value column to a float if needed
                if 'float64' != self._df[self._yName].dtype:
                    self._df[self._yName] = self._df[self._yName].astype('float',
                                                                        errors='ignore')
                # get rid of Nan from the values (y-axis)
                # not strictly necessary, but lack of NaN values tends to make
                # follow on data analysis less problematic
                self._df.dropna(subset=[self._yName], inplace=True)
                
                # If the index name is set to the tsName, and the index data type 
                # is datetime, then the index is all set up. Otherwise make it so...
                if self._tsName != self._df.index.name or 'datetime64[ns]' != self._df.index.dtype:
                    # The name and/or datatype of the index is not as needed.
                    # Reset and redo the index, changing the datatype if needed.
                    self._df.reset_index(inplace=True)
                    if 'datetime64[ns]' != self._df[self._tsName].dtype:
                        # For changing to timestamps, coerce option for errors is marking
                        # dates after midnight (next day) as NaT.
                        # Not sure why. Try it with raise, first, and you get
                        # all the values. Put it in a try block, just in case an error is
                        # raised.
                        try:
                            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                                    errors='raise',
                                                                    box = True, 
                                                                    format=self._sourceTimeFormat,
                                                                    exact=False,
                                                                    #infer_datetime_format = True,
                                                                    origin = 'unix')
                        except:
                            print('    WARNING: Problem converting some timestamps from \
    the source data.  Timestamps may be incorrect, and/or some rows may be missing.')
                            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                                    errors='coerce',
                                                                    box = True, 
                                                                    infer_datetime_format = True,
                                                                    origin = 'unix')
                    # Condition the data before creating the index
                    # get rid of any NaN and NaT timestamps. These can be from the
                    # original data or from invalid conversions to datetime
                    self._df.dropna(subset=[self._tsName], inplace=True)
                    # round the timestamp to the nearest ms. Unseen ns and
                    # fractional ms values are not always displayed, and can cause
                    # unexpected merge and up/downsample results
                    self._df[self._tsName] = self._df[self._tsName].dt.round('L')
                    # get rid of any duplicate timestamps as a result of rounding
                    self._df[self._tsName].drop_duplicates(subset=self._tsName,
                                                           keep='last', inplace=True)
                    # now the data type is correct, and forseen data errors are removed.
                    # Set the index to the timestamp column
                    self._df.set_index(self._tsName, inplace=True)
                    
                # Make sure the index is sorted possible better performance later
                self._df.sort_index(inplace=True)
                # Apply the query string if one is specified.
                # Replace "val" with the column name.
                if self._vq != '':
                    queryStr = self._vq.replace("val", self._yName)
                    # try to run the query string, but ignore it on error
                    try:
                        self._df.query(queryStr, inplace = True)
                    except:
                        print('    WARNING: Invalid query string. Ignoring the \
    specified query.')

                # now the timestamp is the index, so filter based on the specified
                # start and end times
                self._df= self._df.loc[self._startQuery : self._endQuery]

                # Get the inferred frequency of the index. Store this internally,
                # and expose below as a property.  Sometimes the data has repeated
                # timestamps, and infer_freq does not work.Try it, but if it comes up
                # empty, try it manually
                try:
                    # try the inferred frequency
                    inferFreq = pd.infer_freq(self._df.index)
                except:
                    inferFreq = None
                
                # If that did not work, try to get it manually. When timestamps are
                # repeated, it looks like the odd/even rows in that order are
                # repeated.
                if inferFreq is None or inferFreq == pd.Timedelta(0): 
                    print('    Determining sampling frequency manually. \
    Data may have repeated or corrupted timestamps.')
                    # Use 3 and 4 if possible, just in case there is
                    # something strange in the beginning. Otherwise, use entries 0
                    # and 1, or give up, and use 1 second.
                    if len(self._df.index) >= 4:
                        inferFreq = pd.Timedelta((self._df.index[3] -
                                                  self._df.index[2]))
                    elif len(self._df.index) >= 2:
                        inferFreq = pd.Timedelta((self._df.index[1] - self._df.index[0]))
                    else:
                        print('    WARNING: Not enough data to determine the \
    data frequency. Using 1 sec.')
                        inferFreq = pd.Timedelta('1S')

                # At this point, there is value for inferred frequency,
                # but there may be repeated times due to sub-second times being
                # truncated.  If this happens, the time delta will be 0. Deal
                # with it by forcing 1 second
                if inferFreq == pd.Timedelta(0):
                    print('    WARNING: Two rows have the same timestamp. \
    Assuming a 1 second data frequency.')
                    inferFreq = pd.Timedelta('1S')

                # Frequency is ready. Convert it and store it as a time offset.
                self._timeOffset = to_offset(inferFreq)

                # Print interesting stuff as long as length > 0
                if len(self._df.index) > 0:
                    print('    Start Time:', self._df.index[0])
                    print('    End Time:', self._df.index[-1])
                    print('    Frequency:', self._timeOffset)

    def __repr__(self):
        outputMsg=  '{:13} {}'.format('Name: ', self.name + '\n')
        outputMsg+= '{:13} {:18} {:10} {}'.format('Index: ', self._df.index.name, \
'datatype: ', str(self._df.index.dtype) + '\n')
        outputMsg+= 'Columns:\n'
        for col in self.columns:
            outputMsg+= '{:4} {:15} {} {}'.format(' ', col, self.columns[col], '\n')
        outputMsg+= '{:13} {}'.format('Value Query: ', self.valueQuery + '\n')
        outputMsg+= '{:13} {}'.format('Start Time: ', str(self.startTs) + '\n')
        outputMsg+= '{:13} {}'.format('End Time: ', str(self.endTs) + '\n')
        outputMsg+= '{:13} {}'.format('Period: ', str(self.timeOffset) + '\n')
        outputMsg+= '{:13} {}'.format('Length: ', str(self.count) + '\n')
        return(outputMsg)

    def resample(self, resampleArg='S', stats='m'):
        # Resample the data from the complete dataframe.
        # The original data is replaced with the resampled data.
        # Determine if we are up or down sampling by comparing the
        # specified frequency (time offset) with the data frequency.
        # If the data is being upsampled (increase the frequency), than values
        # will be forward filled to populate gaps in the data.
        # If the data is being downsampled (decrease in frequency), then the
        # specified stats will be calculated on values that fall between those
        # being sampled.
        #
        # stats (optional, default='m') Choose which statistics to calculate when
        # resampling. Choices are: (V)alue, m(I)n, ma(X), (a)verage/(m)ean,
        # and (s)tandard deviation. The (a) and (m) options do the same thing.
        # Choices are not case sensitive. Default is 
        # average/mean.  In the case of the Value option, the first value available
        # which is on or after the timestamp is shown. The values between this and the
        # next sample point are thrown away. For the other options, the intermediate
        # values are used to calculate the statistic.
        #
        # Make sure the resample argument is valid
        if resampleArg is None:
            # no sample period specified, use 1 second
            print('    WARNING: ' + self._name + ': No resample period \
specified. Using 1 Second.')
            resampleTo = to_offset('S')
        else:
            try:
                resampleTo = to_offset(resampleArg)
            except:
                print('    WARNING: ' + self._name + ': Invalid resample \
period specified. Using 1 second.')
                resampleTo = to_offset('S')

        if resampleTo < self.timeOffset:
            # Data will be upsampled. We'll have more rows than data.
            # Forward fill the data for the new rows -- a new row will use the
            # previous recorded value until a new recorded value is available.
            # In other words -- carry a value forward until a new one is avail.
            # The stats argument is ignored.
            
            # If stats were specified, print a message about not using the specified stats
            if stats is not None or not stats:
                print('    WARNING: Data is being upsampled. There will be more \
rows than data. \nCalculating statistics on repeated values does not make sense, \
and a non-empty stat parameter was specified.\n The "stats" parameter will be ignored. \n \
Set "stats" to an empty string ("") or "None" to eliminate this warning.\n')

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
                print('    ' + self.name + ': Upsampled from ' \
                    + str(self.timeOffset) + ' to ' + str(resampleTo))
                # update the object frequency
                self._timeOffset = resampleTo
                # now overwrite the original dataframe with the resampled one
                # and delete the resampled one
                self._df = dfResample
                del dfResample
                return
            except:
                print('    WARNING: ' + self._name + ': Unable to resample \
data. Data unchanged. Frequency is ' + str(self.timeOffset))
                print('    Error: ', sys.exc_info())
                return
        elif resampleTo > self.timeOffset:
            # Data will be downsampled. We'll have more data than rows.
            # This means we can calculate statistics on the values between
            # those being displayed.  Use the stats option to determine which
            # stats are to be calculated.

            # make stats not case sensitive
            if stats is not None:
                self._stats = str(stats).lower()
            else:
                self._stats = ''

            # Determine column names.
            # Determine the stat flags. These are used below to decide which
            # columns to make and calculate. Display the stat if the representative
            # character is in the stats argument. Find returns -1 if not found
            displayValStat = self._stats.find('v') > -1   # value
            displayMinStat = self._stats.find('i') > -1   # minimum
            displayMaxStat = self._stats.find('x') > -1   # maximum
            # mean or average
            displayMeanStat = self._stats.find('m') > -1 or self._stats.find('a') > -1
            # standard deviation
            displayStdStat = self._stats.find('s') > -1 or self._stats.find('d') > -1
            # If none of the flags are set, an invalid string must have been
            # passed. Display just the mean, and set the stats string accordingly
            if not displayValStat and not displayMinStat and \
               not displayMaxStat and not displayMeanStat and \
               not displayStdStat:
                displayMeanStat = True
                self._stats = 'm'
                
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
                print('    ' + self.name + ': Downsampled from ' + \
                      str(self.timeOffset) + ' to ' + str(resampleTo))
                # update the object frequency
                self._timeOffset = resampleTo
                # now overwrite the original dataframe with the resampled one
                # and delete the resampled one
                self._df = dfResample
                del dfResample
                return
            except:
                print('    WARNING: ' + self._name + ': Unable to resample \
data. Data unchanged. Frequency is ' + str(self.timeOffset))
                print('    Error: ', sys.exc_info())
                return
        else:
            # resampling not needed. Specified freq matches data already
            print('    ' + self.name + ': Resampling not needed. New frequency \
matches data frequency. Data unchanged. Frequency is ' + str(self.timeOffset))
            return
           
    def appendData(srcDf):
        # TODO:  Implement
        # Enforce or coerce data into compatible structure
        print('    WARNING: appendData() method not implemented. Data  unchanged.')
        return

    def replaceData(srcDf):
        # TODO:  Implement
        # Enforce or coerce data into compatible structure
        print('    WARNING: repalceData() method not implemented. Data  unchanged.')
        return

    # read only properties
    @property
    def name(self):
        return self._name

    @property
    def tsName(self):
        return self._tsName
           
    @property
    def valueQuery(self):
        return self._vq

    @property
    def columns(self):
        # this dictionary will include column names as the key and the data
        # type as a value
        # {col name : datatype, ...}
        return dict(self._df.dtypes)

    @property
    def data(self):
        return self._df

    @property
    def timeOffset(self):
        return self._timeOffset

    @property
    def startTs(self):
        # assumes index is sorted and start is at the top
        return self._df.index[0]

    @property
    def endTs(self):
        # assumes index is sorted and end is at the bottom
        return self._df.index[-1]

    @property
    def count(self):
        return len(self._df.index)

