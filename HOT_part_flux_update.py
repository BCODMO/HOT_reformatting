#!/usr/local/bin/python
desc='''This script uses the functions in HOT_data_extract to process the hot *.flux
data files, and reformat them into csv files for jgofs compatibility. It assumes that 
your current working directory is the 'particle_flux/' directory. It also assumes that 
the data file follows the convention as described in the particle_flux/Readme.flux 
file. The headers are identified as the variable names as listed in the Readme.flux 
file. It assumes that all the files have the same data field names. The process 
overwrites the files specified in the -o option, if they exist.'''

# Python packages:
# numpy,csv,sys,pprint,OptionParser,fnmatch,os,HOT_niskin_data_extract,collections,re
#
# created: mbiddle 20180423
# updated: mbiddle 20180425
#
# History:
# 20180425:
#   - Completed development. Operational now.
#
# 20180423:
#   - Copied CTD processing script to make this

vers="%prog 1.0 - Updated 20180425"
#import csv # reading csv
import sys # for testing
import pprint # to pretty print dictionaries
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

def reorder_ordereddict(od, new_key_order):
    new_od = collections.OrderedDict([(k, None) for k in new_key_order if k in od])
    new_od.update(od)
    return new_od

## Print current working directory
print "Current working directory:",os.getcwd()

## Get the files to be processed:
#---------------------------------------------------------#
if options.test: # subset of the data files
  data_files = ['hot1-12.flux','hot280-288.flux']
  readme='Readme.flux'
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
  import fnmatch
  import os
  sum_files=[]
  for file in os.listdir('../cruise.summaries/'):
    if fnmatch.fnmatch(file,'hot*.sum'):
      sum_files.append('../cruise.summaries/'+file)
  data_files=[]
  for root, subFolders, files in os.walk('.'):
    for filename in fnmatch.filter(files,'hot*.flux'):
      data_files.append(os.path.join(root,filename).replace('./',''))
  if options.verbose:
    print "total summary file count:",len(sum_files)
    print "total data file count:",len(data_files)
#---------------------------------------------------------#
## Pull out all the data using the functions defined above
cruise_sum = HOT_data_extract.process_cruise_sum(sum_files)
data_result = HOT_data_extract.process_part_flux(data_files)#,formats) # requires formats dictionary
#pprint.pprint(data_result)
#sys.exit()
## Now do some post processing
#---------------------------------------------------------#
if options.verbose:
  print "Data successfully ingested, now processing...\n"

i=0 # start an iterator
data_combined={}#collections.OrderedDict()
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
  # Compile the data into a giant dictionary with variables as key and data as values.
  for var in data_result[file_data]: # iterate through data file variables
    if "data" in data_result[file_data][var]: # look for dictionaries with data  
      if var.replace(" ","_") not in data_combined.keys(): # if the variable dictionary is not started replace space w/underscore
        data_combined.update({var.replace(" ","_"):data_result[file_data][var]["data"]})
      else: # otherwise append the data to it
        data_combined[var.replace(" ","_")].extend(data_result[file_data][var]["data"])
## Add latitude and longitude coordinates
data_combined.update({'lon':[-158.00] * len(data_combined[var],)})
data_combined.update({'lat':[22.75] * len(data_combined[var],)})

#sys.exit()
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
  #sort -k23,23n -k6,6 -b -t, part_flux.csv > part_flux_sorted.csv
  #sort by date, then depth
  subprocess.call(["sort","-k26,26n","-k8,8n","-b","-t,",options.out_file], stdout=f)
  print "\nWrote",options.out_file.replace(".csv","_sorted.csv")

    ## Update the datacomments file
  dir_path = options.out_file.rsplit('/',1)[0]+'/'
  print "\nUpdating",dir_path+'part_flux.datacomments'
  import datetime
  now = datetime.datetime.now()
  f = open(dir_path+'part_flux.datacomments','r')
  lines = f.readlines()
  lines[0]="\#  version: %s\n" % now.strftime("%Y-%m-%d")
  f.close()
  f = open(dir_path+'part_flux.datacomments', 'w')
  f.writelines(lines)
 # do the remaining operations on the file
  f.close()

print "\nCompleted HOT_part_flux_update.py." 
