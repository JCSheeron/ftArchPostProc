This program is written in Python 3. The Version most recently used by the author
is 3.6.5, but many other 3.x versions may/should/probably will work.

Once the environment has the correct libraries installed, run the 
the program with the -h option. It looks like this in Linux:
    ./ftArchPostProc.py -h 

The -h option brings up help information for the options.


This program uses the following imported libraries. The environment must be set up
to include them.

system related:
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

user libraries
Note: May need PYTHONPATH (set in ~/.profile?) to be set depending
on the location of the imported files

TimeStamped Indexed Data Class
	from bpsTsIdxData import TsIdxData

list duplication helper functions
	from bpsListDuplicates import listDuplicates 
	from bpsListDuplicates import listToListIntersection


