 ftArchPostProc

 Final Test Archive Data Post Processing
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

 In the case of a time normalized export file (the -n command line argument), the data
 columns are as follows:
 Timestamp, Tag1 Value, Tag2 Value, Tag3 Value ...

 Note: The -h, -n, and -a options are mutually exclusive. One and only one must
 be specified.

 TODO: Update merge related params

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
 Tag1 TimeStamp, Tag1 Value, Tag2 TimeStamp, Tag2Timestamp ...

 -a (required and mutually exclusive with -t and -n). Input file is a
 archive export file. The format is:
 ValueId,Timestamp (YYYY-MM-DD HH:MM:SS.mmm),value,quality,flags

 -n (required and mutuall exclusive with -a and -t). Input file is a time
 normalized file. The format is:
 Timestamp, Tag 1 Value, Tag 2 Value, Tag 3 Value ...

 TODO: Update merge related params

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
 would be:
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
 year, %y two digit year, %H hours (24hr format), %I hours (12 hr format), %M
 minutes, %S seconds, %p AM/PM. The default string is "%m/%d/%Y %I:%M:%S %p".'

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
 message is included unless this argument is specified.

 TODO: Improved Error handling? Testing will tell if this is needed.

 TODO: Include units. This does not come from the data export. One idea is to
 use a JSON file to map tag name with units.  Additionally, a JSON file may be
 used in the archive data (-a option) file to map tag name with ID number. If 
 this is the case, then the same JSON file could be used by both the -t and
 -a options.


 Imports:

 system related
 import sys

 date and time stuff
 from datetime import datetime, time
 from pandas.tseries.frequencies import to_offset
 from dateutil import parser as duparser

 csv file stuff
 import csv

 arg parser
 import argparse

 numerical manipulation libraries
 import numpy as np
 import pandas as pd

 custom libraries
 from TsIdxData import TsIdxData

Details about TsIdxData:

Class: TsIdxData
File: TsIdxData.py

Timestamped Indexed Data

This file implements an data contaner which holds time stamped values,
indexed and sorted by time stamp. The data container is implemented as a
Pandas Dataframe.  The timestamp is a Datetime, and the values are floats. 

The source data used to populate the container can have multiple value
columns, but one column needs have a name which matches yName. This column
will be considered the "value" column.  

The source data used to populate the container also must have one value
column or an index column with a name that matches tsName. This column will be
used as the index.

The constructore is expecting a data frame which is used as the source data.

The constructor (ctor) has these areguments:
  name -- The name to give the object. An instrument name for example.

  tsName -- The name of the timestamp (index) column.

  yName -- The name of the value column.

  df -- The source data used to populate the data container. If a data frame
        is not specified, then an empty data frame is created.  See
        "Data Structure Notes" below.

  valueQuery -- Query string used to filter the dataset.
                Default is empty, so nothing is filtered out. Use "val" to
                represent the process value(s). For example, to filter out
                all values < 0 or > 100, you want to keep everything else,
                so the filter string would be: "val >= 0 and val <= 100".

  startQuery -- Datetime string used to filter the dataset.  Data timestamped
                before this time will be filtered out. The default is empty,
                so no data is filtered out if nothing is specified.

  endQuery -- Datetime string used to filter the dataset.  Data timestamped
              after this time will be filtered out. The default is empty,
              so no data is filtered out if nothing is specified.

  sourceTimeFormat -- Specify the time format of the source data timestamp.
                      If nothing is specified, the value defaults to
                      "%m/%d/%Y %I:%M:%S %p". A string using the following
                      symbolic placeholders is used to specify the format: 
                          %m minutes, %d days, %Y 4 digit year, 
                          %y two digit year, %H hours (24hr format),
                          %I hours (12 hr format), %M minutes, %S seconds,
                          %f for fractional seconds (e.g. %S.%f), %p AM/PM.

DATA STRUCTURE NOTES
  The source data must have the following structure:
      Timestamp data: An index or value column must exist
                      labeled with the tsName specified.  This data must be of 
                      type datetime or convertable to datetime. It will
                      be converted, if needed, to a datetime the format
                      specified with the sourceTimeFormat string.

      Value data:     A value (non-index) column must exist labeled with the 
                      yName specified. This data msut be of type float or
                      or convertable to a float. It will be converted if needed.

                      Other value (non-index) columns are allowed, the names 
                      don't matter, as long as one of them is named yName
                      per above.
     

In addition the data can be resampled.  Resampling makes the most sense when 
the original data has time stamps at regular intervals, and the interval needs
to be changed. If the data is being upsampled (increase the frequency),
than values will be forward filled to populate gaps in the data. If the data
is being downsampled (decrease in frequency), then the specified stats will
be calculated on values that fall between those being sampled.

When resampling, and data is being downsampled, stats can be calculated. The
stats parameter is used to specify which stats to calculate.  It is optional
and defaults to 'm' if not specified. Choices are: (V)alue, m(I)n, ma(X),
(a)verage/(m)ean, and (s)tandard deviation.
The (a) and (m) options do the same thing. Choices are not case sensitive.
Default is average/mean (m).  In the case of the Value option,
the first value available which is on or after the timestamp is shown.
The values between this and the next sample point are thrown away.
For the other options, the intermediate values are used to calculate the
statistic.  Note: The stats parameter is ignored when upsampling.

The following read only properties are implemented
   name
      string -- object name

   tsName
      timestamp -- column name

   valueQuery
      string used to query the source data during construction

   columns
      dictionary with column names as the key and the data type as a value {col name : datatype, ...}

   data
       a copy of the dataframe

   timeOffset
       time period between data samples

    startTs
        start time filter used to query the source data during construction
   
    endTs
       end time filter used to query the source data during construction 

    count
       the number of rows in the data frame 

TODO: There is no way to append or replace data.  The appendData and 
replaceData methods need an implementation. Currently,
if a TsIdxData object is created without specifing a source data frame (df param)
then the object data will be an empty dataframe, and there is no way to populate
it.  

Imports used for TxIdxData.py

imports

system related
import sys
date and time stuff
from datetime import datetime, time
from pandas.tseries.frequencies import to_offset
from dateutil import parser as duparser

numerical manipulation libraries
import numpy as np
import pandas as pd



