# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 01:45:36 2019

@author: sputtaswamy2
"""


import os
import shutil
import logging
import logging.handlers
import configparser
import sys, getopt

LOG_FILENAME = "cut_copy.log"
logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
    )

example_logger = logging.getLogger('main')

# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=5*1024*1024, backupCount=5)

example_logger.addHandler(handler)

# =============================================================================
# 
# read a config file and load all the required parameters.
# 
# =============================================================================

logging.info('Reading log file  ')
config = configparser.ConfigParser()
config.read('pytutorial.ini')
schedule = config['GENERAL']['SCHEDULE_TIME']


input_dir = config['DIRECTORIES']['SOURCE_DIR']

destination_dir = config['DIRECTORIES']['DESTINATION_DIR']

    
def main(argv):
    try:
        opts, args = getopt.getopt(argv,"hi:o:f:",["ifile=","ofile=","flag="])
    except getopt.GetoptError:
        print ('04_parameters.py   -i <inputfile> -o <outputfile>')
        print (str(getopt.GetoptError))
        sys.exit(2)
    for opt, arg in opts:
        
        if opt == '-h':
           print ('04_parameters.py -i <inputfile> -o <outputfile>')
           sys.exit()
        elif opt in ("-i", "--ifile"):
           input_dir = arg
        elif opt in ("-o", "--ofile"):
           destination_dir = arg


    files = os.listdir(input_dir)
    if len(files):
        
        for file in files:
            logging.debug('Copying ' + file)
            shutil.copy(input_dir + '\\' + file , destination_dir )


if __name__ == "__main__":
   main(sys.argv[1:])
   
