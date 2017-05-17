#!/usr/bin/python

import glob
import os
import sys
import xlwt
import csv

retsheet = 'result_zhuangliang.xls'
perffile = '/sum_cron*'
configfile = '*.config.all.txt'
sdrfile = '/avg_max_min_stdev_*'
configlnnum = 11
perflnnum = 7
perf1row = 7
sdr1col = 13
thputwatthr = 24


def validateIP(s):
    sip = s.split('.')
    if len(sip) != 4:
        return False
    for x in sip:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True


def getIPFolderName(path):
    root, dirs, files = os.walk(path).next()
    dirsdot = []
    for idx, val in enumerate(dirs):
        if os.listdir(logdir + '/' + val):
            dirsdot.append((val.replace('-', '.')).replace('tdw.', ''))
        else:
            dirs[idx] = 'deleteME!'
    # remove empty directory
    dirs = [item for item in dirs if "deleteME!" not in item]
    for dir in dirsdot:
        if not validateIP(dir):
            print('Folder name error! Exit(0)...')
            sys.exit(1)
    folderdict = dict(zip(dirs, dirsdot))
    return folderdict


def getPerfRet(file):
    perf = {}
    for dir in file:
        for filename in glob.glob(logdir + '/' + dir + perffile):
            with open(filename, 'r') as f:
                fl = [next(f).strip() for ln in xrange(11)]
            perf[fold[dir]] = fl
    for ser in perf.keys():
        perf[ser].pop(1)
        for idx, val in enumerate(perf[ser]):
            if ':' in val:
                perf[ser][idx] = perf[ser][idx].split(': ')[1].strip()
            if ' secs' in val:
                perf[ser][idx] = perf[ser][idx].replace(' secs', '')
        # merge time value
        perf[ser][8] = perf[ser][8] + ' ' + perf[ser][9]
        perf[ser][5] = perf[ser][5] + ' ' + perf[ser][6]
        perf[ser][2] = perf[ser][2] + ' ' + perf[ser][3]
        # remove duplicated value
        perf[ser].pop(9)
        perf[ser].pop(6)
        perf[ser].pop(3)
        perf[ser].pop(0)
    perfret = perf
    return perfret


def getSdr(p):
    sdr = {}
    for dir in p:
        for filename in glob.glob(logdir + '/' + dir + sdrfile):
            with open(filename, 'r') as f:
                fl = f.read().splitlines()
            fl.pop(0)
            sdr[fold[dir]] = fl
    return sdr


def getConfig(p):
    with open(glob.glob(p + '/' + configfile)[0]) as f:
        fl = f.read().splitlines()
    for i in range(0, 3):
        fl.pop(0)
    if len(fl) % configlnnum != 0:
        print('Config file is corrupted! Exit(0)...')
        sys.exit(1)
    configret = {}
    newlist = []
    for idx, val in enumerate(fl):
        if idx % configlnnum == 0:
            ip = (val.replace('=', '').replace('-', '.')).replace('tdw.', '')
            newlist = []
        else:
            newlist.append(val)
        configret[ip] = newlist
    return configret


def writecsv(fold, conf, perf, sdr):
    '''
    csvfile = file("tmp.csv", "wb")
    writer = csv.writer(csvfile)
    writer.writerow(["IP","CPU","PNOR","BMC","OS","JDK",
        "Compiler","Starts","Ends","SMT","Vcore","Freq",
        "PowerCap","MR#","MR_RT","Spark#","Spark_RT","SQL#",
        "SQL_RT","Scala#","Scala_RT","AvgPower","MaxPower","MinPower",
        "StedevPower","Topops","ActualVcore","CpuUser%","CpuSys%","CpuIdle%"])
    '''
    print "\033[40;36m IP\t\tSMT/Vcore/Freq\tMR#\tMR_RT\tSQL#\tSQL_RT\tMR+SQL#\t#vsRef\tMRSQL/W\t/WvsRef\tMaxPower \033[0m"
    
    # Get Ref server data for compare
    ref_tput = int(perf["100.76.29.3"][0]) + int(perf["100.76.29.3"][2])
    ref_tput_per_watt = ref_tput/float(sdr["100.76.29.3"][0].split()[1])/24
    
    server_nbr = len(sorted(fold.values()))
    datatable = [ [0 for i in range(30)] for j in range(server_nbr) ]
    row = 0
    for ser in sorted(fold.values()):
        datatable[row][0] = ser    # ip
        datatable[row][1] = ""    # cpu core# 
        datatable[row][2] = ""    # PNOR 
        datatable[row][3] = ""    # BMC
        datatable[row][4] = ""    # OS
        datatable[row][5] = conf[ser][8].split(': ')[1]    # JDK
        datatable[row][6] = ""    # compilier
        datatable[row][7] = ""    # start time
        datatable[row][8] = ""    # end time
        datatable[row][9] = int(conf[ser][0].split('=')[1])    # SMT
        datatable[row][10] = int(conf[ser][1].split('=')[1])   # vcore
        datatable[row][11] = conf[ser][2].split('=')[1]   # Freq
        datatable[row][12] = ""   # Power Cap
        datatable[row][13] = perf[ser][0]   # MR#
        datatable[row][14] = perf[ser][1].split(' ')[0]   # MR Time
        datatable[row][15] = ""   # Spark # (SQL+Scala)
        datatable[row][16] = ""   # Spark Time
        datatable[row][17] = perf[ser][2]   # SQL#
        datatable[row][18] = perf[ser][3].split(' ')[0]   #d SQL Time
        datatable[row][19] = perf[ser][4]   # Scala
        datatable[row][20] = perf[ser][5].split(' ')[0]   # Scala Time
        datatable[row][21] = float(sdr[ser][0].split()[0])   # Avg Power
        datatable[row][22] = float(sdr[ser][0].split()[1])   # Max Power
        datatable[row][23] = float(sdr[ser][0].split()[2])   # Min Power
        datatable[row][24] = float(sdr[ser][0].split()[3])   # Stdev Power
        datatable[row][25] = ""   # Topops
        datatable[row][26] = ""   # Actual Vcore#
        datatable[row][27] = ""   # CPU User%
        datatable[row][28] = ""   # CPU Sys%
        datatable[row][29] = ""   # CPU Idle%
        
        MR_SQL_Nbr = float(datatable[row][13]) + float(datatable[row][17])
        MR_SQL_Nbr_vs_Ref = MR_SQL_Nbr/ref_tput if ref_tput != 0 else "NA"
        
        MR_SQL_Nbr_Per_Watt = float(MR_SQL_Nbr)/float(datatable[row][22])/24
        MR_SQL_Nbr_Per_Watt_vs_Ref = MR_SQL_Nbr_Per_Watt/ref_tput_per_watt if ref_tput !=0 else "NA"
        
        '''
        print datatable[row][0], "\t",  \
            datatable[row][9], "/", datatable[row][10], "/", datatable[row][11], "\t", \
            datatable[row][13], "\t", round(float(datatable[row][14])), "\t", \
            datatable[row][17], "\t", round(float(datatable[row][18])), "\t", \
            MR_SQL_Nbr, "\t", round(MR_SQL_Nbr_Per_Watt,2)
        '''

        print "{ip}\t{smt}/{vcore}/{freq}\t{mr_nbr}\t{mr_time}\t{sql_nbr}\t{sql_time}\t{mrsql_nbr}\t{mrsql_nbr_vs_ref}x\t{mrsql_perwatt}\t{mrsql_perwatt_vs_ref}x\t{maxpower}".\
            format(ip=datatable[row][0], smt=datatable[row][9], vcore=datatable[row][10], freq=datatable[row][11],\
            mr_nbr=datatable[row][13], mr_time=round(float(datatable[row][14])),\
            sql_nbr=datatable[row][17], sql_time=round(float(datatable[row][18])),\
            mrsql_nbr=MR_SQL_Nbr, mrsql_nbr_vs_ref=round(MR_SQL_Nbr_vs_Ref,2),\
            mrsql_perwatt=round(MR_SQL_Nbr_Per_Watt,2), mrsql_perwatt_vs_ref=round(MR_SQL_Nbr_Per_Watt_vs_Ref,2),\
            maxpower=datatable[row][22])
          
        row += 1

    #print(datatable)
    #writer.writerows(datatable)
    #csvfile.close()

def wrtExcel(fold, conf, perf, sdr):
    wkb = xlwt.Workbook(encoding='utf-8')
    sheet = wkb.add_sheet('perf_result', cell_overwrite_ok=True)

    # write first text column
    # for idx, val in enumerate(coltxt):
    #     sheet.write(idx, 0, val)

    print "fold \n"
    print(fold)
    print "conf \n"
    print(conf)
    print "perf \n"
    print(perf)
    print "sdr \n"
    print(sdr)
    sys.exit(0)


    raw = 1
    for ser in sorted(fold.values()):
        ''' write system configuration '''
        sheet.write(raw, 0, ser)
        # write JDK
        sheet.write(raw, 2, conf[ser][8].split(': ')[1])
        # write SMT
        sheet.write(raw, 4, int(conf[ser][0].split('=')[1]))
        # write Vcore
        sheet.write(raw, 5, int(conf[ser][1].split('=')[1]))
        # write freq
        sheet.write(raw, 6, conf[ser][2].split('=')[1])

        ''' write performance data '''
        last = perf1row + perflnnum - 1
        for idx in range(perf1row, last):
            if ' ' in perf[ser][idx - perf1row]:
                substrLeft = perf[ser][idx - perf1row].split(' ')[0]
                # substrRight = perf[ser][idx - perf1row].split(' ')[1]
                sheet.write(raw, idx, float(substrLeft))
                # sheet.write(raw + 1, idx, float(substrRight))
            else:
                sheet.write(raw, idx, float(perf[ser][idx - perf1row]))

        # write MR+SQL tasks
        # valmrsql = float(perf[ser][0]) + float(perf[ser][2])
        # sheet.write(raw, last, valmrsql)
        # # write avg task time
        # val1 = (float(perf[ser][0]) * float(perf[ser][1].split()[0]) +
        #         float(perf[ser][2]) * float(perf[ser][3].split()[0]))
        # val2 = float(perf[ser][0]) + float(perf[ser][2])
        # valavgt = val1 / val2
        # sheet.write(raw, last + 1, valavgt)
        # # write thput/watt
        # valthputwatt = valmrsql / (float(sdr[ser][0].split()[1]) * thputwatthr)
        # sheet.write(raw, last + 2, valthputwatt)

        ''' write sdr data '''
        # write sdr title
        # sheet.write(sdr1row, col, 'Avg')
        # sheet.write(sdr1row, col + 1, 'Max')
        # sheet.write(sdr1row, col + 2, 'Min')
        # sheet.write(sdr1row, col + 3, 'Stdev')
        # for idx in range(sdr1row + 1, len(sdr[ser]) + sdr1row + 1):
        #     subraw = raw
        #     for sp in range(0, 4):
        #         # write sdr cell
        #         valsdrcell = float(sdr[ser][idx - (sdr1row + 1)].split()[sp])
        #         sheet.write(subraw, idx, valsdrcell)
        #         subraw += 1
        sheet.write(raw, sdr1col, float(sdr[ser][0].split()[0]))
        sheet.write(raw, sdr1col + 1, float(sdr[ser][0].split()[1]))
        sheet.write(raw, sdr1col + 2, float(sdr[ser][0].split()[2]))
        sheet.write(raw, sdr1col + 3, float(sdr[ser][0].split()[3]))
        raw += 1

    retfile = logdir + '/' + retsheet
    wkb.save(retfile)
    print(retfile + ' is saved.')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: %s <log_folder> <duration>" % sys.argv[0])
        sys.exit(1)

    # set working directory
    logdir = sys.argv[1]
    # set calculate duration
    thputwatthr = int(sys.argv[2])

    # get ip address from directory name
    fold = getIPFolderName(logdir)

    # get values from log files
    confret = getConfig(logdir)
    perfret = getPerfRet(fold.keys())
    sdrret = getSdr(fold.keys())

    # write excel result
    #wrtExcel(fold, confret, perfret, sdrret)
    writecsv(fold, confret, perfret, sdrret)
    sys.exit(0)
