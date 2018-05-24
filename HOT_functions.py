#!/usr/local/bin/python
#
#
## This script pulls in a format identifier file and parses a data file according to that
# format identifier file. This script also pulls in a cruise summary file and parses it
# into a dictionary.
#
# Python Requirements:
# sys
#
# created: mbiddle 20180306
# updated: mbiddle 20180524
#
# History:
# 20180524:
#   - removed functions and incorporated them into individual scripts
#   - renamed to HOT_functions.py
#
#
# 20180425:
#   - Added two new functions; process_prim_prod and process_part_flux.
#   - The two new functions allow for extracting the data from the primary productivity 
#     and particle flux data files according to the specifications defined in the 
#     respective Readme.(pp/flux) files.
#
# 20180402:
#   - Changed file name to HOT_data_extract.py since I will add CTD processing here.
#
# 20180327:
#   - Updated process niskin to include CASTNO on output.
#
# 20180326:
#   - Previous logic for year conversion said if yr > 99 add 2000, year 01 would only 
#     add 99. So, changed to if year less than 80 add 2000 since we don't have data 
#     earlier than 1980.
#
# 20180323:
#   - Updated create_formats_dict to read from the HOT supplied readme file.
#   - removed the list comparison returnNotMatches, since I wasn't using it.
#
# 20180320:
#   - split the reformatting code to another script HOT_niskin_update.py
#   - To use these functions define the following:
#   import HOT_niskin_data_extract
#
#   Then, for example, in your script use:
#   result = HOT_niskin_data_extract.create_formats_dict(format_file)
#
#
#   - All the initial functions are here and useable for other processes.
#
# 20180316
#   - created four functions
#     1. to process the formatting of the data
#     2. to process the data file following the formatting from 1.
#     3. to process the summary files
#     4. to compare two lists and send back missing elements
#   - Working on connecting the data w/ the cruise summary
#   - Trying to determine what to do when expo codes don't match
#
# 20180308
#   - Started working through the file.
#
import sys # for testing

def process_cruise_sum(sum_files):
  '''
  ## This sub-routine opens the cruise.summaries/*.sum files and process them into dictionaries
  # it assumes cruise.summaries is one directory up from where you currently are. For
  # example, you feed it these .gof files:
  # 'hot1.gof'
  # 'hot2.gof'
  #
  # It will look for:
  # '../cruise.summaries/hot1.sum'
  # '../cruise.summaries/hot2.sum'
  #
  # The output is a dictionary with keys identified from the expo code, station number and
  # cast number.
  # 
  #
  '''
  sum_title={}
  sum_head={}
  sum_unit={}
  result={}
  for sum_key in sum_files:
    sumfile = open(sum_key,'r') # open the file
    sum_key=sum_key.replace("../","") # make the key more readable
    ## Process the header of the file
    sum_title[sum_key]=sumfile.readline() # line 1
    sum_head[sum_key]=sumfile.readline() # line 2
    sum_unit[sum_key]=sumfile.readline() # line 3
    sumfile.readline() ## skip the --- line 4
    
    for line in sumfile: # iterate through each data line and parse on position
      ## Error checking to see if identity code already exists
      if line[0:9].strip()+'.'+line[15:20].strip()+'.'+line[20:24].strip() in result.keys(): 
        print line[0:9].strip()+'.'+line[15:20].strip()+'.'+line[20:24].strip(),"is already defined in the dictionary."
        print "Exiting!"
        sys.exit()
      else: # write the summary and do year converstions <-- add station and cast no to this
        result[line[0:9].strip()+'.'+line[15:20].strip()+'.'+line[20:24].strip()]=\
          {'Ship':line[0:9].strip(),\
          'Date':line[30:37].strip(),\
          'Month':int(line[31:33]),\
          'Day':line[33:35].strip(),\
          'Year':int(line[35:37])+2000 if int(line[35:37])<80 else int(line[35:37])+1900,\
          'section':line[9:15].strip(),\
          'timeutc':line[37:42].strip(),\
          'timecode':line[42:46].strip(),\
          'lat':line[46:58],\
          'lon':line[58:71],\
          'nav_code':line[71:76].strip(),\
          'depth_max':line[76:82].strip(),\
          'depth_hgt':line[82:87].strip(),\
          'pres_max':line[87:92].strip(),\
          'num_bottles':line[92:99].strip(),\
          'parameters':line[99:112].strip(),\
          'comments':line[112:].strip(),\
          'bcodmo_comment':'key built as expocode.station.cast',\
          'HOT_summary_file_name':sum_key}
        # simplifying to just save the entire line
#        result[line[0:9].strip()+'.'+line[15:20].strip()+'.'+line[20:24].strip()]=[line]
        # create the dictionary, the key is result[EXPOCODE.STATION.CAST] then all
        # the attributes are pulled from the summary file  If year is less than 80 make it a 2000
  sumfile.close()
  return result;
