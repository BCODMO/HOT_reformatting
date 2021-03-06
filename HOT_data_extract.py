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

## The data OSPREY page can be found at https://www.bco-dmo.org/dataset/3773.

def create_formats_dict(format_file):
  '''## Create a dictionary that defines the data formatting from the 
  # Readme.water.jgof Data Record Format section.
  #
  #Data Record Format: 
  #        Column  Format  Item
  #          1-  8   i6    Station Number
  #          9- 16   i3    Cast Number
  #
  #
  # Column indicates the position in the line where the data is found
  # Format indicates the formatting of that item
  # Item indicates what the name of that field is
  #
  '''
  formats = {}
  with open(format_file,'rb') as formatfile:
    for line in formatfile:
      if "Column  Format" in line.strip(): # start reading at this line
        break
    for line in formatfile:
      # since the format file is not consistant, we have to do some search and replace for
      # spaces and ^I (tab) seperators. 
      line = line.replace('       ',' ')  
      col_num = line.replace('        ',' ').replace('    ','')[0:9].strip().split("-")
      datatype=line.replace('        ',' ').replace('       ',' ')\
          .replace('    ','').replace('    ','       ')[8:13].strip()[0]
      datalength=line.replace('        ',' ').replace('       ',' ')\
          .replace('    ','').replace('    ','       ')[8:13].strip()[1:]
      data_formats= "%s%s" % (datalength,datatype)
      fields = line.replace('        ',' ').replace('       ',' ')\
          .replace('    ','').replace('    ','       ')[14:].strip()
      # create the dictionary to be used later. 
      # dictionary looks like:
      # formats['Station Number'] = {'start':1,'end':8,'type':i6} 
      # since python indexing is odd, we need to start at 1 minus the identified column
      # number in the format. 
      formats[fields]={"start":int(col_num[0])-1,"end":int(col_num[1]),"type":data_formats}
  return formats;

def process_niskin(data_files,formats):
  '''## This function process the data files provided in [data_files] according to the
  # formats identified in [formats] and outputs the data into a dictionary structure.
  #
  ## Data is written into a dictionary under the following structure:
  # result[FILE][VAR]['data']
  # where,
  # FILE is the file name.
  # VAR is the short variable name provided in the data file.
  #
  # There are also dictionaries extracted from the headers of the data files.
  # Those are at the result[FILE] level and have the following names:
  # 
  # "expo_code"
  # "whp_id"
  # "cruise_start"
  # "cruise_end"
  #
  # To print all the "FUCO" data from the hot1.gof data file
  # you would use the following call:
  #
  # for data_point in result['hot1.gof']['FUCO']['data']:
  #   print data_point
  #
  '''
  ## Initialize a bunch of dictionaries
  cruise_info={}
  field_names={}
  units={}
  quality_flag={}
  empty_line={}
  long_name={}
  short_names={}
  short_fmt={}
  flag={}
  result={}
  ident=[]
  for df_key in data_files: # loop through each file
    datafile = open(df_key,'r') # open the file
    ident=[] # for every file, reset the identity list
    ## Collect header information first 5 lines
    cruise_info[df_key] = [datafile.readline()]
    field_names[df_key] = datafile.readline()
    units[df_key] = datafile.readline()
    quality_flag[df_key] = datafile.readline()
    empty_line[df_key] = datafile.readline()
    ## exchange the dictionary keys with the short ones from the files
    # this is a python remapping effort to simplify the keys
    short_names[df_key]={}
    long_name[df_key]={}
    for key in formats:
      short_names[df_key][key]=field_names[df_key][int(formats[key]["start"]):int(formats[key]["end"])].strip()
      # extract out the short field names from the data file itself.
    short_fmt[df_key]={}
    # create a new dictionary for the format with the new short names
    # populates it with the format information from the formatfile identified above
    # [start and end position and data format].
    for k,v in short_names[df_key].items():
      short_fmt[df_key][v]=formats[k]
      # save the full string name in a long_name attribute
      short_fmt[df_key][v]["long_name"]=str(k)
     
    flag[df_key]={}
    for key in short_fmt[df_key]: # parse through each format descriptor in formats to get flags
      # do some special processing for the lone * at the end of the flag line
      if quality_flag[df_key]\
      [int(short_fmt[df_key][key]["start"]):int(short_fmt[df_key][key]["end"])].strip()\
      is not "*":
        flag[df_key][key]=quality_flag[df_key]\
        [int(short_fmt[df_key][key]["start"]):int(short_fmt[df_key][key]["end"])]
      else:
        flag[df_key][key]=quality_flag[df_key]\
        [int(short_fmt[df_key][key]["start"]):int(short_fmt[df_key][key]["end"])].replace("*"," ")

    # initialize final structure with header information
    result[df_key]={"expo_code":str(cruise_info[df_key]).split(" ")[1]}
    result[df_key]["whp_id"]=str(cruise_info[df_key]).split(" ")[6]
    result[df_key]["cruise_start"]=str(cruise_info[df_key]).split(" ")[12]
    result[df_key]["cruise_end"]=str(cruise_info[df_key]).split(" ")[14]

    ## parse the data now, using the formats identified above.
    # iterate through each line of data file
    for line in datafile:
      # parse through each format descriptor in formats
      for key in short_fmt[df_key]:
        # get the data from the line and format it as they described
        # quick and dirty bash line: "cut -c 249-256 hot1.gof"
        # data = eval(line[int(short_fmt[key]["start"]):int(short_fmt[key]["end"])].format(short_fmt[key]["type"]))
        # print out raw line for error checking
        data =\
        line[int(short_fmt[df_key][key]["start"]):int(short_fmt[df_key][key]["end"])]
        if key == "STNNBR" or key == "CASTNO":
          if key == "CASTNO":
            castno = data.strip() # pull out the cast number
          if key == "STNNBR":
            stnbr = data.strip() # pull out the station number
        if "stnbr" in locals() and "castno" in locals():
          ident.append(str(cruise_info[df_key]).split(" ")[1]+"."+stnbr+"."+castno) # create the ident key
          result[df_key]["ident"]= {"data": ident} # write the list to the dictionary
          del stnbr # reset the variable
          del castno # reset the variable
        # check if the key exists in the dictionary
        if key not in result[df_key].keys():
        # if not, initialize the dictionary
          result[df_key][key]={
            "long_name":short_fmt[df_key][key]["long_name"],
            "data":[data],
            "flag":flag[df_key][key],
            "start":int(short_fmt[df_key][key]["start"]),
            "end":int(short_fmt[df_key][key]["end"]),
            "format":short_fmt[df_key][key]["type"]}
        else:
      # if the dict exists, append the data to it
          result[df_key][key]["data"].append(data)
    datafile.close()
  return result;

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
