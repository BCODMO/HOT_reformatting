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
# updated: mbiddle 20180524
#
# History:
# 20180524:
#   - Integrated the function 'process_part_flux' from HOT_data_extract into this script.
#   - Sorting was fixed to sort by cruise, then depth.
#
# 20180425:
#   - Completed development. Operational now.
#
# 20180423:
#   - Copied CTD processing script to make this

vers="%prog 1.5 - Updated 20180524"
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

def process_part_flux(data_files):
  '''## Create a dictionary for the particle flux data files using the formats as described in Readme.flux 
  # Accepts a list variable containing file names (relative paths are okay).
  #
  # explicitly parses line by line based on how the records are identified in Readme.flux
  # No special processing, outputs a dictionary per file, per variable, with data as strings. All 
  # information is retained and transferred via the dictionary.
  '''
#  import collections
  result={}
  data_rec1={}
  data_rec2={}
  data_rec3={}
  for data_key in data_files:
    datafile = open(data_key,'r') # open the file
    filename = data_key
    ## Process the header of the file
    data_rec1[data_key]=datafile.readline().replace("\n","") # line 1
    data_rec2[data_key]=datafile.readline().replace("\n","") # line 2
    data_rec3[data_key]=datafile.readline().replace("\n","") # line 3
    result[data_key]={}
    result[data_key]['P_flux filename']=filename
    ## Data record 1
    result[data_key]['Title']=data_rec1[data_key]
  
    vars=['P_flux_filename','Cruise','Depth','Treatment',\
          'Carbon','Carbon_sd_diff','Carbon_n',\
          'Nitrogen','Nitrogen_sd_diff','Nitrogen_n',\
          'Phosphorus','Phosphorus_sd_diff','Phosphorus_n',\
          'Mass','Mass_sd_diff','Mass_n',\
          'Silica','Silica_sd_diff','Silica_n',\
          'Delta_15N','Delta_15N_sd_diff','Delta_15N_n',\
          'Delta_13C','Delta_13C_sd_diff','Delta_13C_n',\
          'PIC','PIC_sd_diff','PIC_n']
    for var in vars:
      result[data_key][var]={}
   
    ## Data record 3 Units
#    result[data_key]['Units']=data_rec5[data_key]
    result[data_key]['Cruise']['Units']='Number' 
    result[data_key]['Depth']['Units']='Meters'
    result[data_key]['Treatment']['Units']=data_rec3[data_key][13:16]
    result[data_key]['Carbon']['Units']=data_rec3[data_key][17:24]
    result[data_key]['Carbon_sd_diff']['Units']=data_rec3[data_key][25:32]
    result[data_key]['Carbon_n']['Units']=data_rec3[data_key][32:35]
    result[data_key]['Nitrogen']['Units']=data_rec3[data_key][35:42]
    result[data_key]['Nitrogen_sd_diff']['Units']=data_rec3[data_key][43:50]
    result[data_key]['Nitrogen_n']['Units']=data_rec3[data_key][51:52]
    result[data_key]['Phosphorus']['Units']=data_rec3[data_key][53:60]
    result[data_key]['Phosphorus_sd_diff']['Units']=data_rec3[data_key][61:68]
    result[data_key]['Phosphorus_n']['Units']=data_rec3[data_key][68:71]
    result[data_key]['Mass']['Units']=data_rec3[data_key][71:78]
    result[data_key]['Mass_sd_diff']['Units']=data_rec3[data_key][78:86]
    result[data_key]['Mass_n']['Units']=data_rec3[data_key][86:89]
    result[data_key]['Silica']['Units']=data_rec3[data_key][89:96]
    result[data_key]['Silica_sd_diff']['Units']=data_rec3[data_key][97:104]
    result[data_key]['Silica_n']['Units']=data_rec3[data_key][104:107]
    result[data_key]['Delta_15N']['Units']=data_rec3[data_key][107:114]
    result[data_key]['Delta_15N_sd_diff']['Units']=data_rec3[data_key][115:122]
    result[data_key]['Delta_15N_n']['Units']=data_rec3[data_key][122:125]
    result[data_key]['Delta_13C']['Units']=data_rec3[data_key][125:132]
    result[data_key]['Delta_13C_sd_diff']['Units']=data_rec3[data_key][133:140]
    result[data_key]['Delta_13C_n']['Units']=data_rec3[data_key][140:143]
    result[data_key]['PIC']['Units']=data_rec3[data_key][143:150]
    result[data_key]['PIC_sd_diff']['Units']=data_rec3[data_key][151:158]
    result[data_key]['PIC_n']['Units']=data_rec3[data_key][158:161]

    # initialize the 'data' dictionaries
    for item in vars:
       result[data_key][item]['data']=[]

    ## Now go get all the data for each file
    for line in datafile: # iterate through each data line and parse on position
      result[data_key]['P_flux_filename']['data'].append(filename)
      result[data_key]['Cruise']['data'].append(line[0:4].replace("\n",""))
      result[data_key]['Depth']['data'].append(line[8:11].replace("\n",""))
      result[data_key]['Treatment']['data'].append(line[14:15].replace("\n",""))
      result[data_key]['Carbon']['data'].append(line[18:23].replace("\n",""))
      result[data_key]['Carbon_sd_diff']['data'].append(line[25:32].replace("\n",""))
      result[data_key]['Carbon_n']['data'].append(line[32:35].replace("\n",""))
      result[data_key]['Nitrogen']['data'].append(line[35:42].replace("\n",""))
      result[data_key]['Nitrogen_sd_diff']['data'].append(line[43:50].replace("\n",""))
      result[data_key]['Nitrogen_n']['data'].append(line[51:52].replace("\n",""))
      result[data_key]['Phosphorus']['data'].append(line[53:60].replace("\n",""))
      result[data_key]['Phosphorus_sd_diff']['data'].append(line[61:68].replace("\n",""))
      result[data_key]['Phosphorus_n']['data'].append(line[68:71].replace("\n",""))
      result[data_key]['Mass']['data'].append(line[71:78].replace("\n",""))
      result[data_key]['Mass_sd_diff']['data'].append(line[78:86].replace("\n",""))
      result[data_key]['Mass_n']['data'].append(line[86:89].replace("\n",""))
      result[data_key]['Silica']['data'].append(line[89:96].replace("\n",""))
      result[data_key]['Silica_sd_diff']['data'].append(line[97:104].replace("\n",""))
      result[data_key]['Silica_n']['data'].append(line[104:107].replace("\n",""))
      result[data_key]['Delta_15N']['data'].append(line[107:114].replace("\n",""))
      result[data_key]['Delta_15N_sd_diff']['data'].append(line[115:122].replace("\n",""))
      result[data_key]['Delta_15N_n']['data'].append(line[122:125].replace("\n",""))
      result[data_key]['Delta_13C']['data'].append(line[125:132].replace("\n",""))
      result[data_key]['Delta_13C_sd_diff']['data'].append(line[133:140].replace("\n",""))
      result[data_key]['Delta_13C_n']['data'].append(line[140:143].replace("\n",""))
      result[data_key]['PIC']['data'].append(line[143:150].replace("\n",""))
      result[data_key]['PIC_sd_diff']['data'].append(line[151:158].replace("\n",""))
      result[data_key]['PIC_n']['data'].append(line[158:161].replace("\n",""))

  return result;

## Print current working directory
print "Current working directory:",os.getcwd()

## Get the files to be processed:
#---------------------------------------------------------#
if options.test: # subset of the data files
  data_files = ['hot1-12.flux','hot280-288.flux']
  readme='Readme.flux'
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
    for filename in fnmatch.filter(files,'hot*.flux'):
      data_files.append(os.path.join(root,filename).replace('./',''))
  if options.verbose:
    print "total data file count:",len(data_files)
#---------------------------------------------------------#
## Pull out all the data using the functions defined above
data_result = process_part_flux(data_files)

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
  subprocess.call(["sort","-k25,25n","-k6,6n","-b","-t,",options.out_file], stdout=f)
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
  f.close()

print "\nCompleted HOT_part_flux_update.py." 
