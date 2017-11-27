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

# create a TimeStamp Indexed data class
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
            # coerce option for errors is marking dates after midnight (next
            # day) as NaT. Not sure why. Try it with raise, first, and you get
            # all the values. Put it in a try block, just in case an error is
            # raised.
            try:
                self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                errors='raise',
                                                box = True, 
                                                #format = "%m/%d/%Y %H:%M:$S.%f",
                                                #unit = 'ms',
                                                infer_datetime_format = True,
                                                origin = 'unix')
            except:
                print('Problem converting some timestamps.  Rows are probably \
missing')
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
            # and expose below as a property.
            # infer_freq seems to be unreliable. Try it, but if it comes up
            # empty, try it manually
            try:
                # try the inferred frequency
                inferFreq = pd.infer_freq(self._df.index)
            except:
                inferFreq = None
            
            # If that did not work, try to get it manually
            if inferFreq is None or inferFreq == pd.Timedelta(0): 
                print('Determining sampling frequency manually.')
                # Use entries 2 and 3 if you can, just in case there is
                # something strange in the beginning. Otherwise, use entries 0
                # and 1, or give up, and use 1 second.
                if len(self._df.index) >= 3:
                    inferFreq = pd.Timedelta((self._df.index[2] - self._df.index[1]))
                elif len(self._df.index) >= 2:
                    inferFreq = pd.Timedelta((self._df.index[1] - self._df.index[0]))
                else:
                    print('Not enough data to determine the data frequency. Using 1 sec.')
                    inferFreq = pd.Timedelta('1S')

            # At this point, there is value for inferred frequency,
            # but there may be repeated times due to sub-second times being
            # truncated.  If this happens, the time delta will be 0. Deal
            # with it by forcing 1 second
            if inferFreq == pd.Timedelta(0):
                print('Two rows have the same timestamp.  Assuming a \
1 second data frequency.')
                inferFreq = pd.Timedelta('1S')

            
            # Frequency is ready. Convert it and store it as a time offset.
            self._timeOffset = to_offset(inferFreq)

            # troubleshooting
            # print(self._df)
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
        # stats (optional, default='m') Choose which statistics to calculate when
        # resampling. Ignored if not resampling (-rs must be specified for this option
        # to do anything).  Choices are: (V)alue, m(I)n, ma(X), (a)verage/(m)ean,
        # and (s)tandard deviation. Choices are not case sensitive. Default is 
        # average/mean.  In the case of the Value option, the first value available
        # which is on or after the timestamp is shown. The values between this and the
        # next sample point are thrown away. For the other options, the intermediate
        # values are used to calculate the statistic.
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
            displayStdStat = stats.find('s') > -1 or stats.find('d') > -1
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

