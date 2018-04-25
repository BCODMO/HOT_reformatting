#!/usr/local/bin/python
#
#
## This script pulls data from the specific urls in the dictionaries identified in the
# input. The data will be written to directories in your current working directory.
#
# For example, you run the script at:
# test/
#
# and you run it with all the options enabled, like so:
# HOT_niskin_getData.py -s -c -n
#
# You will then transfer, recursively, all the data from the appropriate urls.
# The data will be placed in subdirectories as identified in the url.
# For example,
# data from ftp://mananui.soest.hawaii.edu/pub/hot/ctd/
# will all be in the directory:
# test/ctd/
#
#
# created: mbiddle 20180309
# updated: mbiddle 20180423
#
# History:
# 20180423:
#   - Updated to include getting particle flux and primary production data
#
# 20180406: 
#   - Updated the wget calls to explicitly get the files of interest.
#
# 20180329:
#   - Changed filename from HOT_niskin_getData.py to HOT_getData.py to indicate, more
#     precisely that this script gets the data for niskin, ctd, or summary files not 
#     just niskin.
#
# 20180309
#   - Initialized the script
#   - all the urls are ready, I just need to make sure we are grabbing all the right
#   stuff. Still not sure about the CTD data, will work on that later.
import subprocess
from optparse import OptionParser
import sys

usage = "usage: %prog [options]\n\nNote: [options] are inclusive, you can identify one or all."
version = "%prog 1.0"
parser = OptionParser(usage=usage,version=version)
parser.add_option("-c", "--ctd",
                  action="store_true", dest="ctd",
                  help="To transfer the CTD data")
parser.add_option("-n", "--niskin",
                  action="store_true", dest="niskin",
                  help="To transfer the Niskin data")
parser.add_option("-s","--summary", 
                  action="store_true", dest="summary",
                  help="To transfer the cruise summary data")
parser.add_option("-f","--particle_flux", 
                  action="store_true", dest="particle_flux",
                  help="To transfer the cruise summary data")
parser.add_option("-p","--primary_prod", 
                  action="store_true", dest="primary_prod",
                  help="To transfer the primary productivity data")
(options, args) = parser.parse_args()

url={}
optional={}
if options.ctd:
  url['ctd'] ='ftp://mananui.soest.hawaii.edu/pub/hot/ctd/'
  optional['ctd'] = ['-A','"h*.ctd"','-Ipub/hot/ctd/hot-*']
  # wget -np -N -r -nH --cut-dirs=2 -A "h*.ctd" -Ipub/hot/ctd/hot-* ftp://mananui.soest.hawaii.edu/pub/hot/ctd/
if options.niskin:
  url['niskin']='ftp://ftp.soest.hawaii.edu/dkarl/hot/water/'
  optional['niskin'] = []
  
if options.summary:
  #print "Summary option not available yet."
  url['cruise_sum']='ftp://mananui.soest.hawaii.edu/pub/hot/cruise.summaries/'
  optional['cruise_sum'] = ['-A','"hot*.sum"']

if options.particle_flux:
  url['particle_flux']='ftp://ftp.soest.hawaii.edu/dkarl/hot/particle_flux/'
  optional['particle_flux'] = ['-A','"*.flux"']  

if options.primary_prod:
  url['primary_prod']='ftp://ftp.soest.hawaii.edu/dkarl/hot/primary_production/'
  optional['primary_prod'] = ['-A','"*.pp"']  

print "Transferring:"
for item in url:
  print item,"from",url[item]

response = raw_input('Would you like to continue? (yes/no)\n')
if response.lower() == "no":
  print "Exiting"
  sys.exit()
elif response.lower() == "yes":
  for item in url:
    print "Transferring",item,"from",url[item],'\nThis may take a bit...'
    logfile = "transfer_"+item+".log"
    #print ' '.join(["wget","-np","-N","-r","-nH","--cut-dirs=2","-o"]+[logfile]+optional[item]+[url[item]])
    subprocess.call(' '.join(["wget","-np","-N","-r","-nH","--cut-dirs=2","-o"]+[logfile]+optional[item]+[url[item]]),shell=True)
    print "Check",logfile,"for details on the transfer."
