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
from copy import deepcopy

# import arg parser
import argparse

# import numerical manipulation libraries
import numpy as np
import pandas as pd

# create a TimeStamp Indexed data class
# TODO: Put this def in a module and import it
class TsIdxData(object):
    def __init__(self, name, tsName=None, yName=None, df=None, qs=None):
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
        if qs is None:
            self._qs = ''
        else:
            # something specified for query string (qs)
            # make sure it is a string, and convert to lower case
            self._qs = str(qs).lower()
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
            self._startTs = '01/01/1970 00:00:00'
            self._endTs = '01/01/1970 00:00:00'

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
            # the ts axis will be the index, so get rid of any NaN values
            self._df.dropna(subset=[self._tsName], inplace=True)
            # get rid of Nan from the values (y-axis)
            # not strictly necessary, but lack of NaN values tends to make
            # follow on data analysis less problematic
            self._df.dropna(subset=[self._yName], inplace=True)
            # apply the query string if one is specified.
            # replace "val" and "time" with the column names
            if self._qs != '':
                querystr = self._qs.replace("val", self._yName)
                querystr = querystr.replace("time", self._tsName)
                self._df = self._df.query(querystr)
            # force the columns to have the data types of datetime and float
            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                    errors='coerce')
            self._df[self._yName] = self._df[self._yName].astype('float',
                                                    errors='ignore')
            # set the timestamp as the index
            self._df.set_index(self._tsName, inplace=True)

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
used, it can be specified with the -d or --delimiter option.

Normally, the first row is assumed to be header data (names).
The -noheader option will treat the first row as data (no header).

The data can be filtered if a query string is specified.  The specified string
is used as a query string when the data is populated. The query string is
specified by the -qs or --querystring option.  "val" represents process values
and "time" represents timestamps.

File encoding can be specified with the -e or -encoding option.  Default
encoding is utf_16.\n """

descrStr="Post Processing of historical trend or archive data files."
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
                                 description=descrStr,
                                 epilog=eplStr)
parser.add_argument('inputFileName', help='Input data file (csv)')
parser.add_argument('outputFileName', help= 'Output data file (csv)')
parser.add_argument('-noheader', action='store_true', default=False, \
                   help='Input file does not contain headers.')
parser.add_argument('-d', '--delimiter', default=',', metavar='', \
                   help='Field delimiter. Default is a comma (\",\").')
parser.add_argument('-e', '--encoding', default='utf_16', metavar='', \
                   help='File encoding. Default is utf_16.')
parser.add_argument('-qs', '--queryString', default=None, metavar='', \
                   help='Query string used to filter the dataset. \
Default is empty, so nothing is filtered out. Use "val" to represent the \
process value(s), and use "time" to represent timestamps. For example, to \
filter out all values < 0 or > 100, and before 01/01/2017 you want to keep \
everything else, so the filter string would be "val >= 0 and val <= 100 and \
time >= 01/01/2017".')
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
# args.noheader         True/False  Input data has no header data when set
# args.delimiter        string      Field delimiter. Default is (",")
# args.t                True/False  Historical trend input file type when set
# args.a                True/False  Archive data input file type when set
# args.encoding         string      File encoding. Default is utf_16.
# args.queryString      string      Optional query of the data

# Read the csv file into a data frame.  The first row is treated as the header
df_source = pd.read_csv(args.inputFileName, sep=args.delimiter, 
                    delim_whitespace=False, encoding=args.encoding, header=0, 
                    skipinitialspace=True)#, nrows= 5)
# put source the headers into a list
headerList = df_source.columns.values.tolist()
# make a spot for a list of instrument InstData objects
instData = []

# Iterate thru the header list.
# Create desired column names: value_<instName> and timestamp_<instName>
# Create a instrument data object with data sliced from the big data frame
# look at the -t or -a argument to know what format the data is in 
if args.t and len(headerList) >= 2:
    # historical trend data, and there are at least two (time/value pair) cols
    # In the historical trend case, loop thru every other column to get to the 
    # time stamp columns. The instrument name can be derrived from this and the 
    # values can be obtained from a relative (+1) index from the timestamp
    #for idx in range(0, len(headerList), 2):
    for idx in range(0, 4, 2):
        # For each header entry, make instrument and timestamp column names.
        # Even indexes are timestamps, odd indexes are values.
        # get the inst name, leaving off the bit after the last space, which is
        # normally 'Time' or 'ValueY'
        # rpartition returns a tuple: first, separator, last. Use the first 
        # member as the tag name -- this allows tag names with spaces to be
        # preserved
        instName = headerList[idx].rpartition(' ')[0] 
        # replace the spaces with underscores
        instName = instName.replace(' ', '_')
        # generate timestamp and value field (column) names
        # use the same timestamp name for all, so merge/join operations work
        #tsName = 'timestamp_' + instName
        tsName = 'timestamp'
        valName = 'value_' + instName
        # create a new dataframe for the instrument
        iDframe = pd.DataFrame(df_source.iloc[:,[idx,idx+1]]) 
        # make an object with the instrument name, labels and data frame
        # instrument data object, and append it to the list
        instData.append(TsIdxData(instName, tsName, valName, iDframe,
                                  args.queryString))


    # 
elif args.a and len(headerList) >= 2:
    # archive data, and there are at least two (time/value pair) cols
    # TODO: archive data case
    pass

# as long as there is a list of instrument objects,
# loop thru the instruments and get the first and last datetime
if instData:
    # init the start and end times
    startTime= instData[0].startTs
    endTime= instData[0].endTs
    # find the earliest and latest start/end times
    for inst in instData:
        startTime = min(startTime, inst.startTs)
        endTime = max(endTime, inst.endTs)

# using the start and end times, build an empty  dataframe with the 
# date time range as the index
df_dest = pd.DataFrame(index=pd.date_range(startTime, endTime, freq='s'))
df_dest.columnns=['timestamp']

# as long as there is a list of instrument objects,
# loop thru the instruments and merge the data into the destination data frame
#pd.merge_ordered(df_dest, instData[0]._df, fill_method='ffill', how='outer')
print(df_dest)
print(instData[0]._df)

pd.merge(df_dest, instData[0]._df, on='timestamp', how='outer')
print(df_dest)


"""
# set the data frame with the specified data frame
            self._df = pd.DataFrame(data=df)
            self._df.columns=[self._tsName, self._yName]
            # the ts axis will be the index, so get rid of any NaN values
            self._df.dropna(subset=[self._tsName], inplace=True)
            # get rid of Nan from the values (y-axis)
            # not strictly necessary, but lack of NaN values tends to make
            # follow on data analysis less problematic
            self._df.dropna(subset=[self._yName], inplace=True)
            # apply the query string if one is specified.
            # replace "val" and "time" with the column names
            if self._qs != '':
                querystr = self._qs.replace("val", self._yName)
                querystr = querystr.replace("time", self._tsName)
                self._df = self._df.query(querystr)
            # force the columns to have the data types of datetime and float
            self._df[self._tsName] = pd.to_datetime(self._df[self._tsName],
                                                    errors='coerce')
            self._df[self._yName] = self._df[self._yName].astype('float',
                                                    errors='ignore')
            # set the timestamp as the index
            self._df.set_index(self._tsName, inplace=True)
"""


"""
#
# **** Read from the specified input file
# This is done so the input file can be a properly formed dictionary or a list
# of dictionaries, and the resulting calData should always be a list, perhaps
# with only one element.
calData=[]
with open(args.inputFileName, 'r') as infile:
    inData= json.load(infile)
    infile.close()
    # deal with the list of dictionaryies, or the single dictionary case
    if (type(inData) is list):
        # presumably a list of dictionaries
        # Use extend to add list elements. Append would put a list in a list
        calData.extend(deepcopy(inData))
    elif (type(inData) is dict):
        # A single dictionary entry. Append the dict object to the list.
        # Extend would make a list of dictionary keys.
        calData.append(deepcopy(inData))
    else:
        print('Invalid input data. Exiting.')
        quit()

# **** Loop thru the list from the file.  It should be a list of dictionaries,
# with one list entry per instrument.
for instr in calData:

    # Unpack the dictionary entry into into NumPy
    # friendly datatypes and local variables
    InstName= instr['01_instName']
    EuUnitsLabel= instr['03_EuUnits']
    calDate= instr['02_calDate']
    calNotes= instr['08_notes']

    # **** Get Min and Max values for EU and counts.
    # Counts would be integers, except making them floats allows
    # usage of the numpy.interp and other functions
    minMaxEu = np.array(instr['05_minMaxEu'])
    minMaxCounts = np.array(instr['04_minMaxCounts'])

    # **** Get empirical (actual) values determined during calibration.
    actEus = np.array(instr['07_actEus'], dtype=np.float32)
    actCounts = np.array(instr['06_actCounts'], dtype=np.int32)


    # **** Simulate count values for given EU values if the -s or --simulate
    # option is specified.
    # Create count values based on actual EU values.
    if args.simulate:
        # Generate some fake empirical data. Normally this will be
        # entered up above. For the entered EU values generate some counts
        # values based on the nominal slope and intercept, but include
        # some noise as an artificial error.

        # Use interp to interpolate count values given EU values.
        # For this, the EU is the x-axis, and the counts are the y-axis
        actCounts = np.interp(actEus, minMaxEu, minMaxCounts)
        # and then randomize them a bit
        for idx, actCount in enumerate(actCounts):
             # make sure min < max
            if actCount < 0:
                # make sure min < max
                actCounts[idx] = np.random.randint(actCount * 1.2, \
                                                   actCount * 0.8)
            elif actCount > 0:
                actCounts[idx] = np.random.randint(actCount * 0.8, \
                                                   actCount * 1.2)
            else:
                # at zero
                actCounts[idx]= 0
        # convert the counts back to integers. Round first just to be safe
        actCounts=np.round(actCounts, decimals=0).astype(np.int32)
    # **** End Simulate section

    # **** Generate nominal EU values
    # Generate some EU values at a hand full of count values between min/max
    # Interpolate given the min/max counts and EU values
    nomCounts = np.linspace(minMaxCounts[0], minMaxCounts[1], 5, dtype=np.int32)
    nomEus = np.interp(nomCounts, minMaxCounts, minMaxEu)

    # **** Curve fit the empirical data
    # Curve fit the empirical data to a 1 degree polynomial (a line)
    # polyfit returns the coefficients (highest power first)
    # given the data set and a degree
    coeffs = polyfit(actCounts, actEus, 1)
    # get a polynomial object so we can print it, and so we can get the roots
    # and compensate for a count offset below
    empPoly = np.poly1d(coeffs)
    # make a curve fit line which spans the nominal count values 
    empLine = polyval(coeffs, nomCounts)
    # get curve fit values at count min/max
    empMinMax = polyval(coeffs, minMaxCounts)

    # **** Compensate for a non-zero count at zero EU, which is essentially the
    # x-intercept of the EU axis.  Get the offset, and apply it to the measured
    # count values, and curve fit to get a new formula.
    # Create a 1 dimensional polynomial object, and get the roots. Since this
    # is a single degree polynomial (a line) the root will be a scalar (a
    # single value). The root is the value of x (counts) where Y (EU) is zero.
    countOffset = (empPoly).roots
    # Shift the measured count values by this offset.
    # NOTE: This works because we are using a 1 degree poly fit, and the root
    # is a scalar (single value). Something else may be needed for more
    # complicated curves.
    offsetCounts = (np.round(actCounts - countOffset,
                    decimals=0)).astype(np.int32)
    # make a curve fit for the new line. This is a bit heavy-handed, since this
    # could be done by adjusting count values, and not doing an additional
    # curve fit, but I am doing this as it may be of benefit if we ever need
    # more complicated polynomials as well (degrees > 1), but
    # the roots would not be scalar, so the offset Al Gore rhythm would be
    # different.
    offsetCoeffs = polyfit(offsetCounts, actEus, 1)
    # get a polynomial object so we can print it
    offsetPoly = np.poly1d(offsetCoeffs)
    # make a new line using the new offset curve fit. Span the nominal counts.
    offsetLine = polyval(offsetCoeffs, nomCounts)
    # get offset values at count min/max
    offsetMinMax = polyval(offsetCoeffs, minMaxCounts)

    # **** Create string to write to the terminal or a file depending on the -v
    # and -o arguments. 
    # outputFilePrefix is not empty. If it is empty, then don't write to a
    # file.
    if args.outputFilePrefix != '' or args.v:
        fname = args.outputFilePrefix + '_' + InstName + '.txt'
        outputMsg = '*' * 78 + '\n'
        outputMsg += 'Nominal and Actual Calibration Data\n'
        outputMsg += InstName + '\n'
        outputMsg += calDate + '\n\n'
        outputMsg += 'NOTE: ' + calNotes + '\n\n'
    outputMsg +='{:37} {:9d} {:9d}\n' \
                .format('Min and Max PLC Nominal Counts: ', minMaxCounts[0], \
                                                    minMaxCounts[1])
        outputMsg += '{:37} {:9.2f} {:9.2f} \n\n' \
                .format('Min and Max Nominal EU (' + EuUnitsLabel + '): ', \
                        minMaxEu[0], minMaxEu[1])
        outputMsg +='{:16}  {:30}\n'.format('Measured Counts', \
                                    'Measured EU (' + EuUnitsLabel + ')')
        outputMsg +='{:16}  {:30}\n'.format('_' * 15, '_' * 30)
        # loop thru the counts and print a list of counts vs eu values
        for idx in range(actCounts.size):
            outputMsg +='{: <16d}  {: <30.2f}\n'.format(actCounts[idx], \
                                                          actEus[idx])
        outputMsg += '\nThe least squares fit 1 degree polynomial (line) is:'
        outputMsg += str(empPoly) + '\n\n'

        outputMsg +='Calibrated engineering units for the min and max \
PLC counts are as follows:\n'
        outputMsg +='EU at min and max PLC Counts:  {:11.4f}   {:11.4f}\n\n' \
                .format(empMinMax[0], empMinMax[1])
        outputMsg += 'Compensate for a non-zero count value at zero EU.\n'
        outputMsg += 'Shift the curve fit up or down by the count value of \n'
        outputMsg += 'the zero EU value (the x-intercept of EU axis).\n'
        outputMsg += 'The adjusted count values vs EU values are:\n\n'
        outputMsg +='{:16}  {:30}\n'.format('Adjusted Counts', \
                                    'Measured EU (' + EuUnitsLabel + ')')
        outputMsg +='{:16}  {:30}\n'.format('_' * 15, '_' * 30)
        # loop thru the counts and print a list of counts vs eu values
        for idx in range(actCounts.size):
            outputMsg +='{: <16d}  {: <30.2f}\n'.format(offsetCounts[idx], \
                                                        actEus[idx])
        outputMsg += '\nThe least squares fit 1 degree polynomial (line) \
for the adjusted counts is:'
        outputMsg += str(offsetPoly) + '\n\n'

        outputMsg += 'Calibrated engineering units for the adjusted \n'
        outputMsg += 'min and max PLC counts are as follows:\n'
        outputMsg += 'EU at min and max PLC Counts:  {:11.4f}   {:11.4f}\n' \
                .format(offsetMinMax[0], offsetMinMax[1])
        outputMsg +='*' * 78 + '\n'

        # output to a file if the -o option is used
        if args.outputFilePrefix != '':
            outFile = open(fname, 'a+')
            outFile.write(outputMsg)
            outFile.close()

        # output to the terminal if the -v option is used
        if args.v:
            print(outputMsg)

    # **** End writing to file or the terminal
    #
    # **** Plot the data

    # get a figure and a single sub-plot to allow better control
    # than using no sub-plots
    fig, ax = plt.subplots()

    # set the titles
    fig.suptitle('Nominal and Actual Calibration Curves', \
                fontsize=14, fontweight='bold')
    plt.title(InstName + '    ' + calDate, fontsize=12, fontweight='bold')
    ax.set_xlabel('counts')
    ax.set_ylabel('Engineering Units (EU)\n' + EuUnitsLabel)

    # make additional room for the labels
    plt.subplots_adjust(left=0.18, bottom=0.18)

    # add the data to the plot
    # plot the measurments as points
    ax.plot(actCounts, actEus, color='blue', \
            linewidth=1.0, linestyle='', \
            markersize=2.8, marker='x', label='meas.')
    # plot the nominal line
    ax.plot(nomCounts, nomEus, color='green', \
            linewidth=1.0, linestyle='-', marker='', label='nominal')
    # plot the curve fit line
    ax.plot(nomCounts, empLine, color='red', \
            linewidth=1.0, linestyle='-', marker='', label='crv. fit')
    # plot the offset curve fit line
    ax.plot(nomCounts, offsetLine, color='orange', \
            linewidth=1.0, linestyle='-', marker='', label='offset')

    # set the legend
    ax.legend(loc='upper left', frameon=True)

    # set axis limits. Extend a bit past the min/max values
    countRange = (minMaxCounts[1] - minMaxCounts[0])
    euRange = (minMaxEu[1] - minMaxEu[0])
    plt.xlim(minMaxCounts[0] - (countRange * 0.05), \
             minMaxCounts[1] + (countRange * 0.05))

    plt.ylim(minMaxEu[0] - (euRange * 0.05), \
             minMaxEu[1] + (euRange * 0.05))

    # set x and y ticks

    # create a two line x-axis labeling with the counts on the top and the 
    # percentages on the bottom
    # first get the values (counts)
    xAxVals=np.linspace(minMaxCounts[0], minMaxCounts[1], 5, endpoint = True)
    # force the x axis value to be integers
    xAxValss=xAxVals.astype(np.int32)
    # then use list comprehension to get corresponding percentages
    xAxPct=[(((x - minMaxCounts[0]) / countRange) * 100) for x in xAxVals]
    # now append them into a string
    xAxLabels=[]
    for idx in range(len(xAxVals)):
        xAxLabels.append(str(xAxVals[idx]) + '\n' + str(xAxPct[idx]) + '%')

    plt.setp(ax, \
            xticks=(np.linspace(minMaxCounts[0], \
                    minMaxCounts[1], 5, endpoint = True)),
            xticklabels=xAxLabels,
            yticks=(np.linspace(minMaxEu[0], minMaxEu[1], 9, endpoint = True)))


    # show the grid
    ax.grid(b=True, which='both', linestyle='-.')

    # Save the plot if the outFilePrefix is not empty. If it is empty, don't
    # save the plot.
    if args.outputFilePrefix != '':
        fname= args.outputFilePrefix + '_' + InstName + '.pdf'
        plt.savefig(fname, orientation='portrait', papertype='letter',
                   format='pdf', transparent=False, frameon=False,
                   bbox_inches='tight', pad_inches=0.25)

    # draw the plot if the -v option is set
    if args.v:
        plt.show()
"""
