#!/usr/local/bin/python
desc='''This script uses the functions in HOT_data_extract to process the hot *.pp
data files, and reformat them into csv files for jgofs compatibility. It assumes that 
your current working directory is the 'primary_productivity/' directory. It also 
assumes that the data file follows the convention as described in the 
primary_productivity/Readme.pp file. The headers are identified as the variable names as listed 
in the Readme.pp file. It assumes that all the files have the same data field names. The 
process overwrites the data files specified in the -o option, if they exist.'''

# Python packages:
# numpy,csv,sys,pprint,OptionParser,fnmatch,os,HOT_niskin_data_extract,collections,re
#
# created: mbiddle 20180424
# updated: mbiddle 20180524
#
# History:
# 20180524:
#   - Updated script to include the function 'process_prim_prod' from HOT_data_extract.py
#   - Script deals with newly added date and time fields.
#   - date time also converted to ISO 
#
# 20180425:
#   - Completed development. Is operational now.
#
# 20180424:
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
## The data OSPREY page can be found at https://www.bco-dmo.org/dataset/737163.

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

## Define some functions
def reorder_ordereddict(od, new_key_order):
    new_od = collections.OrderedDict([(k, None) for k in new_key_order if k in od])
    new_od.update(od)
    return new_od

def process_prim_prod(data_files):
  '''## Create a dictionary for the primary productivity data files using the formats as described in Readme.pp 
  # Accepts a list variable containing file names (relative paths are okay).
  #
  # explicitly parses line by line based on how the records are identified in Readme.pp
  # No special processing, outputs a dictionary per file, per variable, with data as strings. All 
  # information is retained and transferred via the dictionary.
  '''
#  import collections
  result={}
  data_rec1={}
  data_rec2={}
  data_rec3={}
  data_rec4={}
  for data_key in data_files:
    datafile = open(data_key,'r') # open the file
    filename = data_key
    ## Process the header of the file
    data_rec1[data_key]=datafile.readline().replace("\n","") # line 1
    data_rec2[data_key]=datafile.readline().replace("\n","") # line 2
    data_rec3[data_key]=datafile.readline().replace("\n","") # line 3
    data_rec4[data_key]=datafile.readline().replace("\n","") # line 4
    datafile.readline() #skip blank line
    result[data_key]={}
    result[data_key]['Prim_prod filename']=filename
    ## Data record 1
    result[data_key]['Title']=data_rec1[data_key]
  
    vars=['PrimProd_filename','Cruise','Incubation_type','Time',\
          'Date','Start_time','End_time','Depth',\
          'Chl_a_mean','Chl_a_sd','Pheo_mean',\
          'Pheo_sd','Light_rep1','Light_rep2',\
          'Light_rep3','Dark_rep1','Dark_rep2',\
          'Dark_rep3','Salt','Prochl','Hetero',\
          'Synecho','Euk','Flag','start_date_time','end_date_time']
    for var in vars:
      result[data_key][var]={}
   
    ## Data record 3 Units
#    result[data_key]['Units']=data_rec5[data_key]
    result[data_key]['start_date_time']['Units']='YYMMDDHHMM'
    result[data_key]['end_date_time']['Units']='YYMMDDHHMM' 
    result[data_key]['Cruise']['Units']='Number' 
    result[data_key]['Incubation_type']['Units']=data_rec4[data_key][6:11]
    result[data_key]['Time']['Units']=data_rec4[data_key][11:18]
    result[data_key]['Date']['Units']=data_rec4[data_key][18:26]
    result[data_key]['Start_time']['Units']=data_rec4[data_key][26:32]
    result[data_key]['End_time']['Units']=data_rec4[data_key][32:38]
    result[data_key]['Depth']['Units']=data_rec4[data_key][38:43]
    result[data_key]['Chl_a_mean']['Units']=data_rec4[data_key][44:50]
    result[data_key]['Chl_a_sd']['Units']=data_rec4[data_key][51:57]
    result[data_key]['Pheo_mean']['Units']=data_rec4[data_key][58:64]
    result[data_key]['Pheo_sd']['Units']=data_rec4[data_key][65:71]
    result[data_key]['Light_rep1']['Units']=data_rec3[data_key][72:79]
    result[data_key]['Light_rep2']['Units']=data_rec3[data_key][80:87]
    result[data_key]['Light_rep3']['Units']=data_rec3[data_key][88:95]
    result[data_key]['Dark_rep1']['Units']=data_rec3[data_key][96:103]
    result[data_key]['Dark_rep2']['Units']=data_rec3[data_key][104:111]
    result[data_key]['Dark_rep3']['Units']=data_rec3[data_key][112:119]
    result[data_key]['Salt']['Units']=data_rec4[data_key][120:128]
    result[data_key]['Prochl']['Units']=data_rec4[data_key][129:136]
    result[data_key]['Hetero']['Units']=data_rec4[data_key][137:144]
    result[data_key]['Synecho']['Units']=data_rec4[data_key][145:152]
    result[data_key]['Euk']['Units']=data_rec4[data_key][153:161]
    result[data_key]['Flag']['Units']=data_rec4[data_key][162:172]

    # initialize the 'data' dictionaries
    for item in vars:
       result[data_key][item]['data']=[]

    ## Now go get all the data for each file
    for line in datafile: # iterate through each data line and parse on position

      date=line[18:26] # YYMMDD (zeros not included)
      start_time=line[26:32] # HHMM
      end_time=line[32:38]  # HHMM

      ## Padding date with zeros and adding century
      if len(str(date.strip()))==6:
         date="%06i"%int(date)
         if int(date[:2]) <=99 and int(date[:2])>=30:
            date="19"+date
         else:
            date="20"+date
      elif date.strip()==-9:
         date="%s"%date.strip()
      else:
         date="%06i"%int(date)
         if int(date[:2]) <=99 and int(date[:2])>=30:
            date="19"+date
         else:
            date="20"+date

      ## Padding time with zeros and combining with date
      # if time > 2400, recalculate time/date to be in standard
      if len(str(start_time.strip()))==4: # start
         start_time="%04i"%int(start_time)
         if int(start_time) > 2400:
           start_date_time="%06i"%(int(date)+1)+"%04i"%(int(start_time)-2400)
         else:
           start_date_time=date+start_time
      elif start_time.strip()=="-9":
         start_time="%s"%start_time.strip()
         start_date_time=start_time
      else:
         start_time="%04i"%int(start_time)
         if int(start_time) > 2400:
           start_date_time="%06i"%(int(date)+1)+"%04i"%(int(start_time)-2400)
         else:
           start_date_time=date+start_time

      if len(str(end_time.strip()))==4: # end
         end_time="%04i"%int(end_time)
         if int(end_time) > 2400:
           end_date_time="%06i"%(int(date)+1)+"%04i"%(int(end_time)-2400)
         else:
           end_date_time=date+end_time
      elif end_time.strip()=="-9":
         end_time="%s"%end_time.strip()
         end_date_time=end_time
      else:
         end_time="%04i"%int(end_time)
         if int(end_time) > 2400:
           end_date_time="%06i"%(int(date)+1)+"%04i"%(int(end_time)-2400)
         else:
           end_date_time=date+end_time

      ## Convert to ISO8601 if its not -9
      if not start_date_time.strip()=="-9":
        start_date_time=start_date_time[0:4]+"-"+start_date_time[4:6]+\
                    "-"+start_date_time[6:8]+"T"+start_date_time[8:10]+\
                    ":"+start_date_time[10:12]+":00"
      if not end_date_time.strip()=="-9":
        end_date_time=end_date_time[0:4]+"-"+end_date_time[4:6]+\
                  "-"+end_date_time[6:8]+"T"+end_date_time[8:10]+\
                  ":"+end_date_time[10:12]+":00"

      ## write the data to the dictionary
      result[data_key]['Date']['data'].append(line[18:26])
      result[data_key]['Start_time']['data'].append(line[26:32])
      result[data_key]['End_time']['data'].append(line[32:38])
      result[data_key]['start_date_time']['data'].append(start_date_time)
      result[data_key]['end_date_time']['data'].append(end_date_time)
      result[data_key]['PrimProd_filename']['data'].append(filename)
      result[data_key]['Cruise']['data'].append(line[0:5].replace("\n",""))
      result[data_key]['Incubation_type']['data'].append(line[6:10].replace("\n",""))
      result[data_key]['Time']['data'].append(line[11:18].replace("\n",""))
      result[data_key]['Depth']['data'].append(line[38:43].replace("\n",""))
      result[data_key]['Chl_a_mean']['data'].append(line[44:50].replace("\n",""))
      result[data_key]['Chl_a_sd']['data'].append(line[51:57].replace("\n",""))
      result[data_key]['Pheo_mean']['data'].append(line[58:64].replace("\n",""))
      result[data_key]['Pheo_sd']['data'].append(line[65:71].replace("\n",""))
      result[data_key]['Light_rep1']['data'].append(line[72:79].replace("\n",""))
      result[data_key]['Light_rep2']['data'].append(line[80:87].replace("\n",""))
      result[data_key]['Light_rep3']['data'].append(line[88:95].replace("\n",""))
      result[data_key]['Dark_rep1']['data'].append(line[96:103].replace("\n",""))
      result[data_key]['Dark_rep2']['data'].append(line[104:111].replace("\n",""))
      result[data_key]['Dark_rep3']['data'].append(line[112:119].replace("\n",""))
      result[data_key]['Salt']['data'].append(line[120:128].replace("\n",""))
      result[data_key]['Prochl']['data'].append(line[129:136].replace("\n",""))
      result[data_key]['Hetero']['data'].append(line[137:144].replace("\n",""))
      result[data_key]['Synecho']['data'].append(line[145:152].replace("\n",""))
      result[data_key]['Euk']['data'].append(line[153:160].replace("\n",""))
      result[data_key]['Flag']['data'].append(line[162:172].replace("\n",""))

  return result;

## Print current working directory
print "Current working directory:",os.getcwd()

## Get the files to be processed:
#---------------------------------------------------------#
if options.test: # subset of the data files
  data_files = ['hot1-12.pp','hot280-288.pp']
  readme='Readme.pp'
  import fnmatch
  import os
  if options.verbose:
    print "total data file count:",len(data_files)
else:
## Pull in the list of files from current working directory
  import fnmatch
  import os
  data_files=[]
  for root, subFolders, files in os.walk('.'):
    for filename in fnmatch.filter(files,'hot*.pp'):
      data_files.append(os.path.join(root,filename).replace('./',''))
  if options.verbose:
    print "total data file count:",len(data_files)
#---------------------------------------------------------#

## Pull out all the data using the functions defined above
data_result = process_prim_prod(data_files)
#---------------------------------------------------------#

## Now do some post processing
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
  #sort -k26,26n -k8,8n -b -t, ../../working/prim_prod/prim_prod.csv
  #sort by Cruise, start time, then depth 
  subprocess.call(["sort","-k25,25n","-k21,21n","-k8,8n","-b","-t,",options.out_file], stdout=f)
  print "\nWrote",options.out_file.replace(".csv","_sorted.csv")

  ## Update the datacomments file
  dir_path = options.out_file.rsplit('/',1)[0]+'/'
  print "\nUpdating",dir_path+'prim_prod.datacomments'
  import datetime
  now = datetime.datetime.now()
  f = open(dir_path+'prim_prod.datacomments','r')
  lines = f.readlines()
  lines[0]="\#  version: %s\n" % now.strftime("%Y-%m-%d")
  f.close()
  f = open(dir_path+'prim_prod.datacomments', 'w')
  f.writelines(lines)
  f.close()

print "\nCompleted HOT_prim_prod_update.py."                         
