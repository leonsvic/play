## play with git cmd -1
## 1st update on dev, based on 0316-2
# test modification on dev-bugfix@0316-1

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
import argparse


def getFolderIPDict(WorkingDir):
    subfolders = os.walk(WorkingDir).next()[1] # check os.walk? to understand the return tuple structure
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

def getServerConfig(FolderIPDict):
    ServerConfigFile = '*.config.all.txt'
    smt = "SMT"
    vcore = "vcore"
    freq = "freq"
    jdk = "jdk"
    javahome = "JAVA_HOME"
    kernel = "tbd"
    bmc = "tbd"
    pnore = "tbd"

    IPReg = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))"
    IPPattern = re.compile(IPReg)
    ServerConfig = {}
    ConfigDict = {}
    with open(glob.glob(os.path.join(WorkingDir, ServerConfigFile))[0]) as f:
        # f like ../0524-24H/2017-05-24.config.all.txt
        for line in f:
            if IPPattern.search(line):
                # once match a line with IP@, read the following lines into a new ConfigDict until next line with IP@
                IP = IPPattern.search(line).group()
                ConfigDict = copy.deepcopy(ConfigDict) if ConfigDict else ConfigDict
                ConfigDict = {}
                ServerConfig[IP] = ConfigDict
            if re.compile(smt).search(line):
                ConfigDict[smt] = line.split("=")[1].strip()
            if re.compile(vcore).search(line):
                ConfigDict[vcore] = line.split("=")[1].strip()
            if re.compile(freq).search(line):
                ConfigDict[freq] = line.split("=")[1].strip()
            if re.compile(r"^"+jdk).search(line):
                ConfigDict[jdk] = line.split("=")[1].strip()
            if re.compile(javahome).search(line):
                ConfigDict[javahome] = line.split(":")[1].strip()

    # print ServerConfig

    # [TODO] could add a cross-check here between ServerConfig vs FolderIPDict

    return ServerConfig

def getServerPerfDict(FolderIPDict):
    ServerPerfFile = "sum_cron*"
    ServerPerfDict, PerfDict = {}, {}

    for (k, v) in FolderIPDict.items():


        PerfDict = copy.deepcopy(PerfDict) if PerfDict else PerfDict
        PerfDict = {}
        ServerPerfDict[v] = PerfDict

        with open(glob.glob(os.path.join(WorkingDir, k, ServerPerfFile))[0]) as f:
            # f like ../0524-24H/tdw-100-76-29-2/sum_cron.2017-05-24.tdw-100-76-29-2
            # only the 1st 10 lines in this file matters,,, could add "failure tasks info" later
            count = 0
            for line in f:
                count += 1
                if count == 3 and re.compile("Total executors mr").search(line):
                    PerfDict["mr_nbr"] = line.split(":")[1].strip()
                    continue
                if count == 4 and re.compile("Average task duration").search(line):
                    PerfDict["mr_time"] = line.split(":")[1].split("sec")[0].strip()
                    continue
                if count == 6:
                    PerfDict["sql_nbr"] = 0
                    if re.compile("Total tasks").search(line):
                        PerfDict["sql_nbr"] = line.split(":")[1].strip()
                    continue
                if count == 7:
                    PerfDict["sql_time"] = 0
                    if re.compile("Average task duration").search(line):
                        PerfDict["sql_time"] = line.split(":")[1].split("sec")[0].strip()
                    continue
                if count == 9:
                    PerfDict["scala_nbr"] = 0
                    if re.compile("Total tasks").search(line):
                        PerfDict["scala_nbr"] = line.split(":")[1].strip()
                    continue
                if count == 10:
                    PerfDict["scala_time"] = 0
                    if re.compile("Average task duration").search(line):
                        PerfDict["scala_time"] = line.split(":")[1].split("sec")[0].strip()
                    continue

    # print ServerPerfDict
    # sys.exit(1)

    return ServerPerfDict

def getServerPowerDict(FolderIPDict):
    PowerFile = "avg_max_min_stdev_*"
    ServerPowerDict, PowerDict = {}, {}
    for (k, v) in FolderIPDict.items():
        PowerDict = copy.deepcopy(PowerDict) if PowerDict else PowerDict
        PowerDict = {}
        ServerPowerDict[v] = PowerDict
        with open(glob.glob(os.path.join(WorkingDir, k, PowerFile))[0]) as f:
            # f like ../0524-24H/tdw-100-76-29-2/avg_max_min_stdev_2017-05-24_24H, only 2nd line is needed for now
            count = 0
            for line in f:
                if count == 1:
                    PowerDict["avg"] = line.split(" ")[0].strip()
                    PowerDict["max"] = line.split(" ")[1].strip()
                    PowerDict["min"] = line.split(" ")[2].strip()
                    PowerDict["stdev"] = line.split(" ")[3].strip()
                if count > 1:
                    break
                count += 1

    return ServerPowerDict

def getServerUtilDict(FolderIPDict):
    sdrfile = "sdr_log*"
    ServerUtilDict, UtilDict = {}, {}
    for (k, v) in FolderIPDict.items():
        UtilDict = copy.deepcopy(UtilDict) if UtilDict else UtilDict
        UtilDict = {}
        ServerUtilDict[v] = UtilDict
        sdr = glob.glob(os.path.join(WorkingDir, k, sdrfile))[0]
        if sdr.find("gz") != -1:
            cmd_gz = "gunzip " + sdr
            print "unzipping sdr file: ", cmd_gz
            os.system(cmd_gz)
            sdr = sdr.split(".gz")[0]
        cmd_getcpu = "grep -A 1 avg-cpu " + sdr + " | grep -v 'avg-cpu' | grep -v '\-\-' " \
                                                      "| awk -F ' ' '{print($1,$2,$3,$4,$5,$6)}' > tmpcpu"
        #cmd_getvcore = "grep -A 1 mr_sparksql_scala_container_coun " + sdr+ \
        #              """ | grep -v mr_sparksql_scala_container_coun | grep -v '\-\-' \
        #             | awk -F' ' '{print($9,$11,$13)}' | sed -e 's/\// /g' >tmpvcore"""

        cmd_getvcore = "grep -E 'CST.*MR.*SQL' " + sdr+ \
                       """ | awk -F' ' '{print($9,$11,$13)}' | sed -e 's/\// /g' >tmpvcore"""

        cmd_getvcoresum = """cat tmpvcore | awk -F' ' '{print $1" "$2" "$3" "$4" "$5" "$1+$3+$5}'>tmpvcoresum"""

        os.system(cmd_getcpu) # create file "tmpcpu" in curent dir
        os.system(cmd_getvcore) # create file "tmpvcore"
        os.system(cmd_getvcoresum) # create file "tmpvcoresum"

        np.set_printoptions(precision=2, suppress=True)
        array = np.loadtxt("tmpcpu", dtype=float, delimiter=" ")
        CpuUtil = np.mean(array, 0)
        array = np.loadtxt("tmpvcoresum", dtype=float, delimiter=" ")
        VcoreUtil = np.mean(array, 0)

        #print CpuUtil, VcoreUtil
        UtilDict["avgVcore"] = VcoreUtil[5]
        UtilDict["avgUser"] = CpuUtil[0]
        UtilDict["avgNice"] = CpuUtil[1]
        UtilDict["avgSys"] = CpuUtil[2]
        UtilDict["avgIO"] = CpuUtil[3]
        UtilDict["avgSteal"] = CpuUtil[4]
        UtilDict["avgIdle"] = CpuUtil[5]

        os.system("rm -rf tmpcpu tmpvcore tmpvcoresum")

    return ServerUtilDict

def getServerYarnDict(FolderIPDict):
    sdrfile = "sdr_log*"

    ServerYarnDumpDict, YarnDumpDict = {}, {}

    for (k, v) in FolderIPDict.items():
        sdr = glob.glob(os.path.join(WorkingDir, k, sdrfile))[0]
        cmdGetNeeded = "grep -P 'CST.*==|Node-ID|Containers|Memory-Used|Memory-Capacity" \
                       "|CPU-Used|CPU-Capacity|(\d{1,2}\.\d{1,2}\ *){6}' " + sdr + " > tmpYarnDump"
        os.system(cmdGetNeeded)

        YarnDumpDict = copy.deepcopy(YarnDumpDict) if YarnDumpDict else YarnDumpDict
        YarnDumpDict = {}
        ServerYarnDumpDict[v] = YarnDumpDict

        with open("tmpYarnDump") as f:
            samplingMatrix, sampling = [], []
            for line in f:
                if re.compile("CST.*==").search(line):
                    if sampling:
                        samplingMatrix.append(sampling)

                    sampling = []
                    for i in range(11):
                        sampling += [0]
                    continue
                if re.compile("(\d{1,2}\.\d{1,2}\ *){6}").search(line):
                    sampling[0] = float(line.strip().split()[0].strip())
                    sampling[1] = float(line.strip().split()[1].strip())
                    sampling[2] = float(line.strip().split()[2].strip())
                    sampling[3] = float(line.strip().split()[3].strip())
                    sampling[4] = float(line.strip().split()[4].strip())
                    sampling[5] = float(line.strip().split()[5].strip())
                    continue
                if re.compile("Containers").search(line):
                    sampling[6] = int(line.split(":")[1].strip())
                    continue
                if re.compile("Memory-Used").search(line):
                    sampling[7] = int(line.split(":")[1].split("MB")[0].strip())
                    continue
                if re.compile("Memory-Capacity").search(line):
                    sampling[8] = int(line.split(":")[1].split("MB")[0].strip())
                    continue
                if re.compile("CPU-Used").search(line):
                    sampling[9] = int(line.split(":")[1].split("vcores")[0].strip())
                    continue
                if re.compile("CPU-Capacity").search(line):
                    sampling[10] = int(line.split(":")[1].split("vcores")[0].strip())
                    continue

        np.set_printoptions(precision=2, suppress=True)
        YarnDumpAvg = np.mean(samplingMatrix, 0)
        YarnDumpDict["cpu_user"] = YarnDumpAvg[0]
        YarnDumpDict["cpu_nice"] = YarnDumpAvg[1]
        YarnDumpDict["cpu_sys"] = YarnDumpAvg[2]
        YarnDumpDict["cpu_io"] = YarnDumpAvg[3]
        YarnDumpDict["cpu_steal"] = YarnDumpAvg[4]
        YarnDumpDict["cpu_idle"] = YarnDumpAvg[5]

        YarnDumpDict["containers"] = YarnDumpAvg[6]
        YarnDumpDict["mem-used"] = YarnDumpAvg[7]
        YarnDumpDict["mem-all"] = YarnDumpAvg[8]
        YarnDumpDict["cpu-used"] = YarnDumpAvg[9]
        YarnDumpDict["cpu-all"] = YarnDumpAvg[10]

    return ServerYarnDumpDict

def getServerHottubDict(FolderIPDict):
    htfile = "hottub_log*"
    ServerHTDict, HTDict = {}, {}

    for (k, v) in FolderIPDict.items():
        if len(glob.glob(os.path.join(WorkingDir, k, htfile))) == 0:
                print k, "is not hottub JDK, skipping hottub metrics collection.."
                continue
        htlog = glob.glob(os.path.join(WorkingDir, k, htfile))[0]

        HTDict = copy.deepcopy(HTDict) if HTDict else HTDict
        HTDict = {}
        ServerHTDict[v] = HTDict
        count = 0
        with open(htlog) as f:
            for line in f:
                count += 1
                if count == 1 and not re.compile("total_crash_cnt").search(line):
                    print "warning: hottub log format may be changed, aborting hottub info gathering.."
                    return
                if count == 2:
                    datalist = line.replace(" ", "").strip().split(",")
                    HTDict["value"] = datalist
    return ServerHTDict


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


def writeCSV(FolderIPDict, ServerConfig, ServerPerfDict, ServerPowerDict, ServerUtilDict, ServerYarnDict, ServerHottubDict):
    # print "quick-view" table title line
    print WorkingDir
    print "\033[40;36m IP\tSMT/Vcore/Freq\tMR#\tMR_RT\tSQL#\tSQL_RT\tMR+SQL#\t#vsRef\tMRSQL/W\t/WvsRef\tMaxPowr\t" \
          "AcuVcor\tUser%\tSys%\tIdle%\tYCon/CPU YMem(G)\tJDK \033[0m"

    # Get Ref server data for compare
    ref_tput = 0
    if ServerPerfDict.get("100.76.29.3"):
        ref_tput = int(ServerPerfDict["100.76.29.3"]["mr_nbr"]) + int(ServerPerfDict["100.76.29.3"]["sql_nbr"])
        ref_tput_per_watt = ref_tput/float(ServerPowerDict["100.76.29.3"]["max"])/int(Duration)

    # start to fill the pre-built 30 x N table
    server_nbr = len(sorted(FolderIPDict.values()))
    datatable = [ [0 for i in range(35)] for j in range(server_nbr) ]
    row = 0
    for server in sorted(FolderIPDict.values()):
        datatable[row][0] = server    # ip
        datatable[row][1] = ""    # cpu core#
        datatable[row][2] = ""    # PNOR
        datatable[row][3] = ""    # BMC
        datatable[row][4] = ""    # OS
        datatable[row][5] = ServerConfig[server]["JAVA_HOME"]   # JDK
        datatable[row][6] = ""    # compilier
        datatable[row][7] = ""    # start time
        datatable[row][8] = ""    # end time
        datatable[row][9] = int(ServerConfig[server]["SMT"])    # SMT
        datatable[row][10] = int(ServerConfig[server]["vcore"])   # vcore
        datatable[row][11] = ServerConfig[server]["freq"]   # Freq
        datatable[row][12] = ""   # Power Cap
        datatable[row][13] = ServerPerfDict[server]["mr_nbr"] if ServerPerfDict[server]["mr_nbr"] else 0  # MR#
        datatable[row][14] = ServerPerfDict[server]["mr_time"] if ServerPerfDict[server]["mr_time"] else 0  # MR Time
        datatable[row][15] = ""   # Spark # (SQL+Scala)
        datatable[row][16] = ""   # Spark Time
        datatable[row][17] = ServerPerfDict[server]["sql_nbr"] if ServerPerfDict[server]["sql_nbr"] else 0   # SQL#
        datatable[row][18] = ServerPerfDict[server]["sql_time"] if ServerPerfDict[server]["sql_time"] else 0 # SQL Time
        datatable[row][19] = ServerPerfDict[server]["scala_nbr"] if ServerPerfDict[server]["scala_nbr"] else 0
        datatable[row][20] = ServerPerfDict[server]["scala_time"] if ServerPerfDict[server]["scala_time"] else 0
        datatable[row][21] = float(ServerPowerDict[server]["avg"]) if ServerPowerDict[server]["avg"] else 0   # Avg Power
        datatable[row][22] = float(ServerPowerDict[server]["max"]) if ServerPowerDict[server]["max"] else 0  # Max Power
        datatable[row][23] = float(ServerPowerDict[server]["min"] ) if ServerPowerDict[server]["min"] else 0  # Min Power
        datatable[row][24] = float(ServerPowerDict[server]["stdev"] ) if ServerPowerDict[server]["stdev"] else 0  # Stdev Power
        datatable[row][25] = ""   # Topops
        datatable[row][26] = round(float(ServerUtilDict[server]["avgVcore"]))   # Actual Vcore#
        datatable[row][27] = round(float(ServerUtilDict[server]["avgUser"])/100, 4)  # CPU User%
        datatable[row][28] = round(float(ServerUtilDict[server]["avgSys"])/100, 4)  # CPU Sys%
        datatable[row][29] = round(float(ServerUtilDict[server]["avgIdle"])/100, 4)   # CPU Idle%
        datatable[row][30] = "" # column Experiment
        datatable[row][31] = int(round(ServerYarnDict[server]["containers"]))# containers
        datatable[row][32] = int(round(ServerYarnDict[server]["cpu-used"]))# cpu-used
        datatable[row][33] = int(round(ServerYarnDict[server]["mem-used"]))/1000 # mem-used, in GB unit
        datatable[row][34] = "" # blank for now

        MR_SQL_Nbr = float(datatable[row][13]) + float(datatable[row][17])
        MR_SQL_Nbr_vs_Ref = MR_SQL_Nbr/ref_tput if ref_tput != 0 else 0

        MR_SQL_Nbr_Per_Watt = float(MR_SQL_Nbr)/float(datatable[row][22])/int(Duration) if datatable[row][22] !=0 else 0
        MR_SQL_Nbr_Per_Watt_vs_Ref = MR_SQL_Nbr_Per_Watt/ref_tput_per_watt if ref_tput != 0 else 0


        # start to print the "quick-view" table content
        print "{ip}\t{smt}/{vcore}/{freq}\t{mr_nbr}\t". \
            format(ip=datatable[row][0].split(".")[2] + "." + datatable[row][0].split(".")[3], smt=datatable[row][9], vcore=datatable[row][10], freq=str(datatable[row][11])[0:4], mr_nbr=datatable[row][13]),
        printc(int(round(float(datatable[row][14]))), 62) # MR Time in color

        print "\t{sql_nbr}\t".format(sql_nbr=datatable[row][17]),
        printc(int(round(float(datatable[row][18]))), 39) # SQL Time in color

        printc2(int(MR_SQL_Nbr), 80000)

        print "\t{mrsql_nbr_vs_ref}x\t{mrsql_perwatt}\t{mrsql_perwatt_vs_ref}x\t{maxpower}" \
              "\t{actualvcore}\t{usercpu}%\t{syscpu}%\t{idlecpu}%\t{yarnConCpu}\t{yarnMem}\t{jdk}". \
            format(mrsql_nbr_vs_ref=round(float(MR_SQL_Nbr_vs_Ref),2), \
                   mrsql_perwatt=round(float(MR_SQL_Nbr_Per_Watt),2), \
                   mrsql_perwatt_vs_ref=round(float(MR_SQL_Nbr_Per_Watt_vs_Ref),2), \
                   maxpower=int(datatable[row][22]), actualvcore=int(datatable[row][26]), \
                   usercpu=datatable[row][27]*100, syscpu=datatable[row][28]*100, idlecpu=datatable[row][29]*100, \
                   yarnConCpu=str(datatable[row][31]) + "/" + str(datatable[row][32]), yarnMem=datatable[row][33], \
                   jdk=str(datatable[row][5])[-16:])
        row += 1

    print

    # print hottub metrics
    if not len(ServerHottubDict) == 0:

        print "\033[40;36m Hottub Metrics \033[0m"
        print "\033[40;36m IP\t\tAllCras\tTskCras\tFailCtn\tGCLimit\t" \
              "MrReuse\tMrFBack\tMrNew\tSqlReus\tSqlFBac\tSqlNew\tConvention \033[0m"


        for k, v in ServerHottubDict.items():
            print v["value"][0], #IP
            print "\t", v["value"][1], # all crash
            print "\t", v["value"][2], # task crash
            print "\t", v["value"][3], # failed container
            print "\t", v["value"][4], # GC limit
            print "\t", v["value"][5], # MR reuse
            print "\t", v["value"][7], # MR fall back
            print "\t", v["value"][9], # MR new
            print "\t", v["value"][6], # SQL reuse
            print "\t", v["value"][8], # SQL fall bacll
            print "\t", v["value"][10], # SQL new
            print "\t", v["value"][11] # Conventional

    # dump csv input only when -c is set
    if dumpCSVFlag:
        print "\033[40;36m\nCSV Input: \033[0m"
        for i in range(server_nbr):
           for j in range(35):
              print "\b{item},".format(item=datatable[i][j]),
           print

def reCalculate(FolderIPDict):
    mrappfile = "mr_appinfo*"
    pd.set_option("precision", 2)
    outputMatrix = []
    for k in sorted(FolderIPDict.keys()):
        # locate the mr_appinfo file then read into pd
        if len(glob.glob(os.path.join(WorkingDir, k, mrappfile))) == 0:
            print WorkingDir, k, " missing mr_appinfo... skipping"
            continue
        mrapp = glob.glob(os.path.join(WorkingDir, k, mrappfile))[0]
        df = pd.read_csv(mrapp)

        # strip the blank in the column name
        i = 0
        while i < len(df.columns):
            df.rename(columns = {df.columns[i] : df.columns[i].strip()}, inplace=True)
            i += 1

        # add 2 columns
        df["extraexec"] = df.executors - df.tasks
        df["extratime"] = df.duration2 if df.avgtask.equals(0) \
            else df.duration2 * (df.executors - df.tasks) / df.executors

        row = []
        for i in range(12):
            row += [0]
        row[0] = date[0:2] + "/" + date[2:4] # date
        row[1] = hour # hour
        row[2] = k.split("tdw-")[1] # ip
        row[3] = tag # tag, poc or td3 or hto

        row[4] = df.duration2.sum()# sum of duration2 -- original numerator (including failed tasks' time)
        row[5] = df.tasks.sum()# sum of tasks -- original denominator (not including failed tasks' number sum)
        row[6] = df.executors.sum()# sum of executors

        row[7] = df[df.extraexec > 0].extraexec.sum()# sum of extra-executors
        row[8] = round(df.extratime.sum(),1)# sum of extra-time  -- need to be subtracted from numerator

        row[9] = round(float(row[4])/float(row[5]),1)# orginal MR RT
        row[10] = round((row[4] - row[8])/row[5], 1)# corrected new MR RT
        row[11] = round(row[9]/row[10],2) # org_RT/new_RT
        outputMatrix.append(row)

    #for row in outputMatrix:
    #    print row
    if dumpCSVFlag:
        with open('/mactmp/recal.csv', 'a+') as f:
            writer = csv.writer(f)
            writer.writerows(outputMatrix)
    else:
        for row in outputMatrix:
            print row



if __name__ != "__main__":
    sys.exit(1)

# verify the command options
parser = argparse.ArgumentParser()
parser.add_argument("dir",
                    help="The directory that contains all machines' one-day log")
parser.add_argument("duration",
                    type=int, choices=[12, 24],
                    help="Legacy option, no use for now, will be removed later")
parser.add_argument("-c", nargs="?", default="0", const="1",
                    help="Optional, default off, will dump into /mactmp/recal.csv if enabled")

args = parser.parse_args(sys.argv[1:])
dumpCSVFlag = int(args.c)

WorkingDir = sys.argv[1]
Duration = sys.argv[2]

foldername = WorkingDir.split("/")[-1]
date = foldername[0:4]
hour = foldername.split("-")[1]
tag = "POC" if len(foldername.split("-"))==2 else "-".join(foldername.split("-")[2:])


# start to work
FolderIPDict = getFolderIPDict(WorkingDir)
reCalculate(FolderIPDict)




'''
ServerConfig = getServerConfig(FolderIPDict)
ServerPerfDict = getServerPerfDict(FolderIPDict)
ServerPowerDict = getServerPowerDict(FolderIPDict)
ServerUtilDict = getServerUtilDict(FolderIPDict)

ServerYarnDict = getServerYarnDict(FolderIPDict)

ServerHottubDict = getServerHottubDict(FolderIPDict)

writeCSV(FolderIPDict, ServerConfig, ServerPerfDict, ServerPowerDict, ServerUtilDict, ServerYarnDict, ServerHottubDict)
'''



