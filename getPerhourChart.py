#!/usr/bin/python

import glob
import os
import sys
import xlwt
import csv
import re # regular expression
import copy
import numpy as np
import pandas as pd
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

def plot(df):
    iplist = df["ip"].drop_duplicates().tolist()
    for ip in iplist:
        """
        df_plot = df[df["ip"] == ip][["hour", "cpu_nonidle%", "vcore%", "mem_used"]]
        ax = df_plot.plot(x="hour", kind="line", figsize=(8,3), title="vcore% vs cpu% - " + date + " - " + ip,
                     xticks=[0, 3, 6, 9, 12, 15, 18, 24], yticks=[0.2, 0.4, 0.6, 0.8, 1.0, 1.2], grid=True,
                     secondary_y="mem_used"),

        #ax.right_ax.set_ylabel("used memory")
        #ax.right_ax.yaxis.set_ylabel("used memory")
        """

        df["yarn_mem"] = df["yarn_mem_used"]/1000

        df_plot = df[df["ip"] == ip][["hour", "cpu_nonidle%", "vcore%"]]
        df_plot.plot(x="hour", kind="line", figsize=(8, 3), title="vcore% vs cpu% - " + date + " - " + ip,
                     xticks=[0, 3, 6, 9, 12, 15, 18, 21, 24], yticks=[0.2, 0.4, 0.6, 0.8, 1.0, 1.2], grid=True)

        df_plot = df[df["ip"] == ip][["hour", "mem_used", "yarn_mem"]]
        df_plot.plot(x="hour", kind="line", figsize=(8, 3), title="memory - " + date + " - " + ip,
                     xticks=[0, 3, 6, 9, 12, 15, 18, 21, 24], grid=True, color=["red", "black"])

        #df_plot = df[df["ip"] == ip]["mem_used"]
        #ax = df_plot.plot(kind="bar", secondary_y="mem_used", color="grey", grid=True)
        #ax.set_ylabel("used memory")
        #ax.yaxis.set_ticks([100, 200, 300, 400, 500])
        #ax.xaxis.reset_ticks()


#def joinPerhourActiveVcore(df):



def createPerHourCSV(FolderIPDict, outputcsv):
    # create a output file named after the date under /mactmp/dailyPerHourUtil for all machines of that day
    # e.g. /mactmp/dailyPerHourUtil/0711-24H.csv

    # check whether created already
    if os.path.exists(outputcsv):
        print outputcsv, "already created, go plotting."
        df = pd.read_csv(outputcsv)
        plot(df)
        return


    # merge all util.csv into one df
    count = 0
    for (k, v) in FolderIPDict.items():
        count += 1
        utilfile = os.path.join(WorkingDir, k, "util.csv")
        if not os.path.exists(utilfile):
            print utilfile, "doesn't exit, skip for this machine.. "
            continue

        df = pd.read_csv(utilfile)
    #    print df.groupby(df.ip).size()
        if count == 1:
            df_all = df
        else:
            df_all = pd.concat([df_all, df])

    #print df_all.groupby(["ip", "hour"]).size()

    # seperate date time
    df_all["day"] = df_all.date.map(lambda x: "-".join(x.split()[1:3])) #
    df_all["weekday"] = df_all.date.map(lambda x: x.split()[0]) #
    df_all["year"] = df_all.date.map(lambda x: x.split()[5]) #
    df_all["cpu_nonidle%"] = (100 - df_all["cpu_idle"])/100
    df_all["vcore%"] = df_all["yarn_cpu_used"]/df_all["yarn_cpu_all"]
    """
    #df_all.groupby(["day", "ip", "hour"]).\
        agg({"cpu_user" : np.average,
             "cpu_nice" : np.average,
             "cpu_sys": np.average,
             "cpu_io": np.average,
             "cpu_steal": np.average,
             "cpu_idle": np.average,
             "ctn#": np.average,
             "yarn_mem_used": np.average,
             "yarn_mem_all": np.average,
             "yarn_cpu_used": np.average,
             "yarn_cpu_all": np.average,
             "mem_total": np.average,
             "mem_used": np.average,
             "mem_free": np.average,
             "mem_share": np.average,
             "mem_cache": np.average,
             "mem_avail": np.average,
             "swap_total": np.average,
             "swap_used": np.average,
             "swap_free": np.average}).sort_index().to_csv(outputcsv)
    """

    df_output = df_all.groupby(["day", "ip", "hour"]).agg(np.average)
    #df_output = df_all.groupby(["day", "ip", "hour"]).mean()
    df_output.to_csv(outputcsv)
    print count, "machines's util csv are added into", outputcsv

    # start to plot
    df = pd.read_csv(outputcsv)
    #joinPerhourActiveVcore(df)

    plot(df)

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
#date = WorkingDir.split("/")[-1][0:8]  # get the last dir in the full path use [-1], then use [0:4] to get the 1st 4
date = WorkingDir.split("/")[-1]  # get the last dir in the full path use [-1], then use [0:4] to get the 1st 4
if not re.compile("[0-1][0-9][0-3][0-9]").search(date):
    print "missing date info in the dir-name:"
    print WorkingDir.split("/")[-1]
    sys.exit()


FolderIPDict = getFolderIPDict(WorkingDir)

outputcsv = "/mactmp/dailyPerHourUtil/" +date + ".csv"
createPerHourCSV(FolderIPDict, outputcsv)









