#!/usr/local/bin/python
desc='''This script uses the functions in HOT_data_extract to process the HOT.gof
data files, combine them with the appropriate cruise information, and export it as one
large csv. It assumes that your current working directory is the niskin 'water/' directory
and you have the cruise.summary files in a directory '../cruise.summaries/' relative to
the 'water/' directory. It also assumes that there is a 'Readme.water.jgofs' file which
contains the Data Record Format in the UH format. 
The headers are identified as the variable names as listed in the data files.
If the data files have differing variable names, the process will exit and signify which
records did not match. The procedure only writes out data for records which have cruise 
summary information. If the cruise summary information does not exist in all of the 
cruise summary files, the procedure will write out the specific identifiers it could not 
find to the terminal window.'''

# Python packages:
# numpy,csv,sys,pprint,OptionParser,fnmatch,os,HOT_niskin_data_extract,collections,re
#
# created: mbiddle 20180320
# updated: mbiddle 20180406
#
# History:
# 20180406:
#   - Updated to include writing of niskin.datacomments
# 
#
# 20180328:
#   - updated to preform the sort to get data to jgofs similar to previous representation
#   - sorted by cruise_name, station number, cast number, and (reversed) rosette in that order.
#
# 20180327:
#   - updated to print out variables alphabetically.
#   - no longer printing bco-dmo generated metadata, only reorganizing what has been provided.
#
# 20180323:
#   - updated to point to the Readme.water.jgof file instead of the hand made csv file for
#   data read formats.
#
# 20180322:
#   - Added functionality to extract summary information and do some conversions of
#   lat/lon, removing special characters and commas.
#   - The process now outputs all the information in unique columns instead of one lumped
#   field of cruise summary information.
#
# 20180321:
#   - completed pulling all the data in, joining together based on identity key, and exporting as a large csv file.
# 20180320:
#   - Pulled the processing of the data out from the function definitions to simplify processing.

vers="%prog 1.2 - Updated 20180406"
#import csv # reading csv
import sys # for testing
#import pprint # to pretty print dictionaries
from optparse import OptionParser # create options for script
import HOT_data_extract # processing the data files functions
import collections # to keep dictionaries organized
import re # regular expressions
import os # operating system
import subprocess # to make bash calls
## The data OSPREY page can be found at https://www.bco-dmo.org/dataset/3773.

## Create optional flags for execution: 
parser = OptionParser(description=desc,version=vers)
parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose",
                  help="Increase verbosity")
parser.add_option("-t", "--test",
                  action="store_true", dest="test",
                  help="Testing procedure with only two files hot1.gof and hot35.gof")
parser.add_option("-o","--out_file",
                  dest="out_file",metavar="FILE",
                  help="write data to FILE")
(options, args) = parser.parse_args()

## Print current working directory
print "Current working directory:",os.getcwd()

## Get the files to be processed:
#---------------------------------------------------------#
if options.test: # subset of the data files
  data_files = ['hot1.gof','hot35.gof']
  readme='Readme.water.jgofs'
  import fnmatch
  import os
  sum_files=[]
  for file in os.listdir('../cruise.summaries/'): # still want all summary info
    if fnmatch.fnmatch(file,'hot*.sum'):
      sum_files.append('../cruise.summaries/'+file)
  if options.verbose:
    print "total data file count:",len(data_files)
    print "total summary file count:",len(sum_files)
else:
## Pull in the list of files from current working directory
  readme='Readme.water.jgofs'
  import fnmatch
  import os
  sum_files=[]
  for file in os.listdir('../cruise.summaries/'):
    if fnmatch.fnmatch(file,'hot*.sum'):
      sum_files.append('../cruise.summaries/'+file)
  data_files=[]
  for file in os.listdir('.'):
    if fnmatch.fnmatch(file,'hot*.gof'):
      data_files.append(file)
  if options.verbose:
    print "total summary file count:",len(sum_files)
    print "total data file count:",len(data_files)
#---------------------------------------------------------#

## Pull out all the data using the functions defined above
formats = HOT_data_extract.create_formats_dict(readme)
cruise_sum = HOT_data_extract.process_cruise_sum(sum_files)
data_result = HOT_data_extract.process_niskin(data_files,formats) # requires formats dictionary

## Now do some post processing
#---------------------------------------------------------#
if options.verbose:
  print "Data successfully ingested, now processing...\n"
### Performing the matching up between summary and data:
cruise_sum_key=[]
for value in cruise_sum.values():
   cruise_sum_key.extend(value.keys())
cruise_sum_keys=sorted(set(cruise_sum_key))
cruise_sum_keys.extend(['EXPOCODE','cruise_name']) # to add in additional export data
#sys.exit()


missing_sum=[]
i=0 # start an iterator
data_combined=collections.OrderedDict()
for file_data in data_result: # iterate through data dictionary for each file
  # Do some initial error checking for variable names
  if i == 0: # use the first file as the master variable list
    master_head=data_result[file_data].keys() # get variable list
    master_file = file_data # get variable name
  else: # for the rest of the files, compare to the master
    if cmp(master_head,data_result[file_data].keys()) != 0: # 0 means they match
      print "Error in variable name comparison."
      # print master_file+":\n",master_head,"\n",file_data+":\n",data_result[file_data].keys()
      # Printing in two columns
      print "Variables of",master_file,"!=",file_data+":"
      fmt = '{:<20}{:<20}'
      print(fmt.format(master_file, file_data))
      print "================================"
      for i, (master, data) in enumerate(zip(master_head, data_result[file_data].keys())):
        print(fmt.format(master, data))
      print '\nProcess exiting.'
      sys.exit() # bail out of script
  i+=1 # increment iterator
  ## starting dictionaries for cruise summary information
  for cruise_sum_key in cruise_sum_keys:
     data_result[file_data][cruise_sum_key]={}
     data_result[file_data][cruise_sum_key]["data"]=[]
  for var in data_result[file_data]: # iterate through data file variables
    if "data" in data_result[file_data][var]: # look for dictionaries with data (variables and identity)
      if var is "ident": # find the identity variable
        # for each entry in that variable (this is a list)
        for ident_data in data_result[file_data]["ident"]["data"]: 
          if ident_data in cruise_sum.keys():
            #only use the data that has matching identity values to output the data
            #Shortcut to add the cruise summary information verbatim:
            #for cruise_sum_key in cruise_sum_keys:
            #  data_result[file_data][cruise_sum_key]["data"].append('%s'\
            #  %(cruise_sum[ident_data][cruise_sum_key]))

            #Longcut, to format and adjust cruise summary information to fit jgofs reqs
            data_result[file_data]["Ship"]["data"].append('%s'\
            %(cruise_sum[ident_data]['Ship']))
            data_result[file_data]["cruise_name"]["data"].append('%s'\
            %(cruise_sum[ident_data]['Ship'][4:].split("/")[0]))
            data_result[file_data]["EXPOCODE"]["data"].append('%s'\
            %(cruise_sum[ident_data]['Ship'].replace("/","_")))
            data_result[file_data]["Date"]["data"].append('%s'\
            %(cruise_sum[ident_data]['Date']))
            data_result[file_data]["Month"]["data"].append('%s'\
            %(cruise_sum[ident_data]['Month']))
            data_result[file_data]["Day"]["data"].append('%s'\
            %(cruise_sum[ident_data]['Day']))
            data_result[file_data]["Year"]["data"].append('%s'\
            %(cruise_sum[ident_data]['Year']))
            data_result[file_data]["timeutc"]["data"].append('%s'\
            %(cruise_sum[ident_data]['timeutc']))
            data_result[file_data]["timecode"]["data"].append('%s'\
            %(cruise_sum[ident_data]['timecode']))
            data_result[file_data]["section"]["data"].append('%s'\
            %(cruise_sum[ident_data]['section']))
            data_result[file_data]["nav_code"]["data"].append('%s'\
            %(cruise_sum[ident_data]['nav_code']))
            data_result[file_data]["depth_max"]["data"].append('%s'\
            %(cruise_sum[ident_data]['depth_max']))
            data_result[file_data]["depth_hgt"]["data"].append('%s'\
            %(cruise_sum[ident_data]['depth_hgt']))
            data_result[file_data]["pres_max"]["data"].append('%s'\
            %(cruise_sum[ident_data]['pres_max']))
            data_result[file_data]["num_bottles"]["data"].append('%s'\
            %(cruise_sum[ident_data]['num_bottles']))
            data_result[file_data]["parameters"]["data"].append('%s'\
            %(cruise_sum[ident_data]['parameters'].replace(',',';')))
            data_result[file_data]["HOT_summary_file_name"]["data"].append('%s'\
            %(cruise_sum[ident_data]['HOT_summary_file_name']))
            data_result[file_data]["bcodmo_comment"]["data"].append('%s'\
            %(cruise_sum[ident_data]['bcodmo_comment']))
            ## Reformatting some of the cruise summary data
            # if no text in comments, replace with ' '
            data_result[file_data]["comments"]["data"].append(\
               ' ' if not \
               re.match('[A-Za-z]','%s'%(cruise_sum[ident_data]['comments'].strip())) else\
               '%s'%(cruise_sum[ident_data]['comments'].replace(',',';').strip()))
            #data_result[file_data]["lat"]["data"].append('%s'%(cruise_sum[ident_data]['lat']))# no conversion
            # convert lat from DD MM.MMM H to (+-)DD.DDDD
            # # [0:4] degrees, [4:10] decimal minutes, [10:12] Hemisphere. 
            data_result[file_data]["lat"]["data"].append(\
             '%s%6.4f'\
             %('-' if 'S' in cruise_sum[ident_data]['lat'][10:12] else '',\
             float(cruise_sum[ident_data]['lat'][0:4])+\
             float(cruise_sum[ident_data]['lat'][4:10])/60)) # writing and converting
            #data_result[file_data]["lon"]["data"].append('%s'%(cruise_sum[ident_data]['lon']))# no conversion
            # convert lon from DDD MM.MM H to (+-)DDD.DDDD
            # [0:5] degrees, [5:11] decimal minutes, [11:13] Hemisphere.
            data_result[file_data]["lon"]["data"].append(\
             '%s%6.4f'\
             %('-' if 'W' in cruise_sum[ident_data]['lon'][11:13] else '',\
             float(cruise_sum[ident_data]['lon'][0:5])+\
             float(cruise_sum[ident_data]['lon'][5:11])/60)) 
          else:
            for cruise_sum_key in cruise_sum_keys:
              # stick in an identifier for cruise summaries that can't be found
              data_result[file_data][cruise_sum_key]["data"].append('MISSING cruise.sum info')
            missing_sum.append(ident_data) # identifiers that can't be found

# Compile the data into a giant dictionary with variables as key and data as values.
for file_data in data_result: # iterate through the files
  for var in data_result[file_data]: # iterate through data file variables
    if "data" in data_result[file_data][var]: # look for dictionaries with data  
      if var.replace(" ","_") not in data_combined.keys(): # if the variable dictionary is not started replace space w/underscore
        data_combined.update({var.replace(" ","_"):data_result[file_data][var]["data"]})
      else: # otherwise append the data to it
        data_combined[var.replace(" ","_")].extend(data_result[file_data][var]["data"])

# remove variables we don't need
del data_combined['ident'] 
del data_combined['bcodmo_comment']

index_to_remove=[]
for i in range(len(data_combined['Ship'])):
  # find location of missing cruise summary information
  if data_combined['Ship'][i] == "MISSING cruise.sum info":
   index_to_remove.append(i) # create a list of indexes that should be removed

for var in data_combined:
  for index in sorted(index_to_remove, reverse=True):
    # remove the data that doesn't have cruise summary
    del data_combined[var.replace(" ","_")][index]

# if there are missing summary records, print out the missing code.
if len(sorted(set(missing_sum)))>=1:
  print 'The following identifiers [expocode.station.cast] do not exist in the'
  print 'cruise summaries files and will not be written to the output file:'
  for value in sorted(set(missing_sum)):
    print value
else:
  if options.verbose:
    print 'All data file identifiers [expocode.station.cast] were found in the cruise summary files.'

## Do some verbose printing:
if options.verbose:
  print "\nFound",len(data_combined.keys()),"variables:"
  for var in data_combined.keys():
    if var in cruise_sum_keys:
      print var,'<== from cruise summary'
    else:
      print var

## sort the dictionary alphabetically
data_combined=collections.OrderedDict(sorted(data_combined.items(), key=lambda t: t[0]))

if options.out_file:
  ## write out the data to ../HOT_niskin.csv
  print "\nWriting to",options.out_file
  import csv
  zd = zip(*data_combined.values())
  with open(options.out_file, 'wb') as f:
    writer = csv.writer(f, delimiter=',',lineterminator='\n')
    writer.writerow(data_combined.keys())
    writer.writerows(zd) #will not write data if the row numbers don't match, should add a check
  print '\nSorting the data file for jgofs...'
  f = open(options.out_file.replace(".csv","_sorted.csv"),"w")
  #sort -k91,91 -k79,79n -k8,8n -k73,73rn -b -t, niskin.csv > niskin_sorted.csv
  subprocess.call(["sort","-k91,91n","-k79,79n","-k8,8n","-k73,73rn","-b","-t,",options.out_file], stdout=f)
  print "\nWrote",options.out_file.replace(".csv","_sorted.csv")

    ## Update the datacomments file
  dir_path = options.out_file.rsplit('/',1)[0]+'/'
  print "\nUpdating",dir_path+'niskin.datacomments'
  import datetime
  now = datetime.datetime.now()
  f = open(dir_path+'niskin.datacomments','r')
  lines = f.readlines()
  lines[0]="\#  version: %s\n\#\n" % now.strftime("%Y-%m-%d")
  f.close()
  f = open(dir_path+'niskin.datacomments', 'w')
  f.writelines(lines)
  # do the remaining operations on the file
  f.close()

print '\nHOT_niskin_update.py complete.'

