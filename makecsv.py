#!/usr/bin/python

import glob
import os
import sys
import xlwt
import csv
import re # regular expression
import copy
import numpy as np
from pyspark.sql import SparkSession



def getFolderIPDict(WorkingDir):
    subfolders = os.walk(WorkingDir).next()[1]  # check os.walk? to understand the return tuple structure
    validSubfolders, IPList = [], []
    for i, value in enumerate(subfolders):
        try:
            if value.index("tdw") == 0:
                # check whether the server dir is empty firstly
                if not os.listdir(os.path.join(WorkingDir, value)):
                    print value + " folder is empty, skip it..."
                    continue
                validSubfolders.append(value)
                IPList.append((value.replace("-", ".")).replace("tdw.", ""))
        except ValueError:
            print "Something wrong with sub-folders names, should be in such format: tdw-100-76-29-3"
            sys.exit(1)
            # if goes here, means "twd" not found in some of the sub-folder, probably structure wrong

    FolderIPDict = dict(zip(validSubfolders, IPList))
    return FolderIPDict


def getServerYarnDict(FolderIPDict):
    sdrfile = "sdr_log*"

    for (k, v) in FolderIPDict.items():

        # check if csv is already created
        outputfile = os.path.join(WorkingDir, k, "util.csv")

        # if len(glob.glob(os.path.join(WorkingDir, k, utilfile))) > 0:
        if os.path.exists(outputfile):
            print outputfile, "already created, skip.."
            continue

        # create csv
        sdr = glob.glob(os.path.join(WorkingDir, k, sdrfile))[0]
        cmdGetNeeded = "grep -P 'CST.*==|Node-ID|Containers|Memory-Used|Memory-Capacity" \
                       "|CPU-Used|CPU-Capacity|(\d{1,2}\.\d{1,2}\ *){6}|Mem:|Swap:' " + sdr + " > tmpYarnDump"
        os.system(cmdGetNeeded)

        with open("tmpYarnDump") as f:
            samplingMatrix, sampling = [], []
            samplingMatrix.append("date,ip,hour,cpu_user,cpu_nice,cpu_sys,cpu_io,cpu_steal,cpu_idle,"
                            "ctn#,yarn_mem_used,yarn_mem_all,yarn_cpu_used,yarn_cpu_all,"
                            "mem_total,mem_used,mem_free,mem_share,mem_cache,mem_avail,"
                            "swap_total,swap_used,swap_free".split(","))
            for line in f:
                if re.compile("CST.*==").search(line):
                    if sampling:
                        samplingMatrix.append(sampling)

                    sampling = []
                    for i in range(23):
                        sampling += [0]

                    sampling[0] = " ".join(line.split()[1:7]) # full date
                    sampling[1] = k # IP
                    sampling[2] = line.split()[4].split(":")[0] # hour

                    continue
                if re.compile("(\d{1,2}\.\d{1,2}\ *){6}").search(line):
                    sampling[3] = float(line.strip().split()[0].strip())
                    sampling[4] = float(line.strip().split()[1].strip())
                    sampling[5] = float(line.strip().split()[2].strip())
                    sampling[6] = float(line.strip().split()[3].strip())
                    sampling[7] = float(line.strip().split()[4].strip())
                    sampling[8] = float(line.strip().split()[5].strip())
                    continue
                if re.compile("Containers").search(line):
                    sampling[9] = int(line.split(":")[1].strip())
                    continue
                if re.compile("Memory-Used").search(line):
                    sampling[10] = int(line.split(":")[1].split("MB")[0].strip())
                    continue
                if re.compile("Memory-Capacity").search(line):
                    sampling[11] = int(line.split(":")[1].split("MB")[0].strip())
                    continue
                if re.compile("CPU-Used").search(line):
                    sampling[12] = int(line.split(":")[1].split("vcores")[0].strip())
                    continue
                if re.compile("CPU-Capacity").search(line):
                    sampling[13] = int(line.split(":")[1].split("vcores")[0].strip())
                    continue
                if re.compile("Mem:").search(line):
                    sampling[14] = line.split()[1].strip()  # Total
                    sampling[15] = line.split()[2].strip()  # used
                    sampling[16] = line.split()[3].strip()  # free
                    sampling[17] = line.split()[4].strip()  # shared
                    sampling[18] = line.split()[5].strip()  # cache
                    sampling[19] = line.split()[6].strip()  # available
                if re.compile("Swap:").search(line):
                    sampling[20] = line.split()[1].strip()  # Total
                    sampling[21] = line.split()[2].strip()  # used
                    sampling[22] = line.split()[3].strip()  # free


        with open(outputfile, 'a+') as f:
            writer = csv.writer(f)
            writer.writerows(samplingMatrix)
            print len(samplingMatrix), "rows are written into", outputfile

    os.system("rm -rf tmpYarnDump")
    return


def printc(rt, target):
    if rt <= target:
        print "\033[40;32m", rt, " \033[0m", # print in green color if meet target, otherwise red
    else:
        print "\033[40;31m", rt, " \033[0m",

def printc2(rt, target):
    if rt >= target:
        print "\033[40;32m",rt, " \033[0m", # print in green color if meet target, otherwise red
    else:
        print rt,



if __name__ != "__main__":
    sys.exit(1)

if len(sys.argv) != 3:
    print("Usage: %s <name_of_the_folder_that_contains_sub-folder_for_each_machine> <12 or 24>" % sys.argv[0])
    sys.exit(1)

WorkingDir = sys.argv[1]
Duration = sys.argv[2]

# extract the date info from the directory name which stores one-day data
date = WorkingDir.split("/")[-1][0:4]  # get the last dir in the full path use [-1], then use [0:4] to get the 1st 4
if not re.compile("[0-1][0-9][0-3][0-9]").search(date):
    print "missing date info in the dir-name:"
    print WorkingDir.split("/")[-1]
    sys.exit()


FolderIPDict = getFolderIPDict(WorkingDir)
getServerYarnDict(FolderIPDict)






