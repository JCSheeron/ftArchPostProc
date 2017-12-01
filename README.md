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

 TODO: Update TsIdxData.py class general/explanation comments


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

