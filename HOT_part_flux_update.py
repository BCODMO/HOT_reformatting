#!/usr/local/bin/python
desc='''This script uses the functions in HOT_data_extract to process the hot *.ctd
data files, and reformat them into csv files for jgofs compatibility. It assumes that 
your current working directory is the ctd 'ctd/' directory and you have the 
cruise.summary files in a directory '../cruise.summaries/' relative to the 'ctd/' 
directory. It also assumes that the data file follows the convention as described in 
the ctd/Readme.format file. The headers are identified as the variable names as listed 
in the data files. It assumes that all the files have the same data field names. The 
procedure only writes out data for records which have cruise summary information. If 
the cruise summary information does not exist in all of the cruise summary files, the 
procedure will write out the specific identifiers it could not find to STDOUT. The 
process overwrites the files in the directory specified in the -d option, if they 
exist.'''

# Python packages:
# numpy,csv,sys,pprint,OptionParser,fnmatch,os,HOT_niskin_data_extract,collections,re
#
# created: mbiddle 20180423
# updated: mbiddle 20180423
#
# History:
# 20180423:
#   - Copied CTD processing script to make this

vers="%prog 0.5 - Updated 20180423"
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
parser.add_option("-d","--dir_path",
                  dest="dir_path",metavar="DIR",
                  help="write data to DIR path")
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
pprint.pprint(data_result)
sys.exit()
## Now do some post processing
#---------------------------------------------------------#
if options.verbose:
  print "Data successfully ingested, now processing...\n"

import os
#data_combined={}
found_ident=[]
data_fields=[]
for file in data_result: # for each data file
  data_combined={}
  ## Checking for the cruise summary info
  ident = data_result[file]['EXPOCODE'].strip()+\
          "."+data_result[file]['Station number'].strip()+\
          "."+data_result[file]['Cast number'].strip()
  if ident not in cruise_sum.keys(): # checking expocode
    print ident,"from file",file,"not found in cruise summary"
  else:
    found_ident.append(ident)
    cruise_sum[ident]['CTD_filename']=data_result[file]['CTD filename'].replace('.ctd','.csv')
#    print cruise_sum[ident] # get all cruise summary information
    for var in data_result[file]: # iterate through data file variables
      if "data" in data_result[file][var]: # look for dictionaries with data (variables and identity)
        data_combined[var.strip()]=data_result[file][var]['data'] # create final directory for writing
    if options.dir_path: # if you want to write the data
      out_file = options.dir_path+data_result[file]['CTD filename'].replace('.ctd','.csv')
      try: # create directory
        os.makedirs(options.dir_path+data_result[file]['CTD filename'].split("/")[0])
      except OSError:
        pass
      ## write out the data to ../../working/ctd
      data_fields.extend(data_combined.keys())
      import csv
      zd = zip(*data_combined.values())
      with open(out_file, 'wb') as f:
        writer = csv.writer(f, delimiter=',',lineterminator='\n')
        writer.writerow(data_combined.keys())
        writer.writerows(zd) #will not write data if the row numbers don't match, should add a check

## provide the desired order of items for top level file
desired_order_list=["cruise_name","station","cast","depth_max","timecode","HOT_summary_file_name","parameters","num_bottles","section","lon","comments","Date","Day","EXPOCODE","lat","nav_code","pres_max","depth_hgt","Month","timeutc","Year","Ship","CTD_filename"]

## Create the top level file from the cruise summary information
if options.dir_path:
  try:
    os.remove(options.dir_path+'ctd_toplevel.dat')# delete top level file if it exists
  except OSError:
    pass

  if len(found_ident)>0:
    import csv
    count=0
    cruise_sum2={}
    with open(options.dir_path+'ctd_toplevel.dat','a') as ftop: # write out top level file
      writer = csv.writer(ftop, delimiter=',',lineterminator='\n')
      for item in found_ident:
        cruise_sum[item]['station']=item.split('.')[1]
        cruise_sum[item]['cast']=item.split('.')[-1]
        cruise_sum[item]['comments']=' ' if not \
                 re.match('[A-Za-z]','%s'%(cruise_sum[item]['comments'].strip())) else\
                 '%s'%(cruise_sum[item]['comments'].replace(',',';').strip())
        # convert lat from DD MM.MMM H to (+-)DD.DDDD
        # # [0:4] degrees, [4:10] decimal minutes, [10:12] Hemisphere.
        cruise_sum[item]['lat']='%s%6.4f'\
               %('-' if 'S' in cruise_sum[item]['lat'][10:12] else '',\
               float(cruise_sum[item]['lat'][0:4])+\
               float(cruise_sum[item]['lat'][4:10])/60) # writing and converting
        # convert lon from DDD MM.MM H to (+-)DDD.DDDD
        # [0:5] degrees, [5:11] decimal minutes, [11:13] Hemisphere.
        cruise_sum[item]['lon']='%s%6.4f'\
               %('-' if 'W' in cruise_sum[item]['lon'][11:13] else '',\
               float(cruise_sum[item]['lon'][0:5])+\
               float(cruise_sum[item]['lon'][5:11])/60)    
        cruise_sum[item]["cruise_name"]='%s'\
              %(cruise_sum[item]['Ship'][4:].split("/")[0])
        cruise_sum[item]["EXPOCODE"]='%s'\
              %(cruise_sum[item]['Ship'].replace("/","_"))
        cruise_sum[item]["parameters"]='%s'\
              %(cruise_sum[item]['parameters'].replace(',',';'))
        # reorder the dictionary 
        cruise_sum2[item]=reorder_ordereddict(cruise_sum[item],desired_order_list)
        del cruise_sum2[item]['bcodmo_comment'] # remove this item
        first_line=cruise_sum2[item].keys() # get first header line
        first_line[-1:]=[">"] # replace last element with > for top level file
        if count==0: # write two line header and first data line
          writer.writerow(first_line)
          #writer.writerow(data_combined.keys())
          writer.writerow(sorted(set(data_fields),reverse=True)) # reverse for sorting
          writer.writerow(cruise_sum2[item].values())
        else:
          writer.writerow(cruise_sum2[item].values())
        count=count+1
    print '\nSorting the top level file for jgofs...'
    f = open(options.dir_path+'ctd_toplevel_sorted.dat',"w")
    #sort -k1,1n -k2,2n -k3,3n -b -t, ctd_toplevel.dat > ctd_toplevel2.dat
    subprocess.call(["sort","-k1,1n","-k2,2n","-k3,3n","-b","-t,",options.dir_path+'ctd_toplevel.dat'], stdout=f)
    print "\nWrote",options.dir_path+'ctd_toplevel_sorted.dat'

  print "\nUpdating",options.dir_path+'ctd.datacomments'
  ## Update the datacomments file
  import datetime
  now = datetime.datetime.now()
  f = open(options.dir_path+'ctd.datacomments','r')
  lines = f.readlines()
  lines[0]="\#  version: %s\n\#\n" % now.strftime("%Y-%m-%d")
  f.close()
  f = open(options.dir_path+'ctd.datacomments', 'w')
  f.writelines(lines)
  # do the remaining operations on the file
  f.close()

print "\nCompleted HOT_ctd_update.py." 
