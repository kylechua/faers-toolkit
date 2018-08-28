from collections import Counter
import numpy as np
import pandas as pd
import sys
import cmath
import math
import time

from package.utils import progressbar as prog
from package.faers import queryhelper as sqlh
from package.faers import signal_scores as ss
from timeit import default_timer as timer

# info
# --[drug] drug (dict)
#   --['all'] all indications (dict)
#     --['pids'] primaryids (list)
#     --['aes'] adverse events (counter)
#     --['stats'] stats (dict)
#       --[ae] each AE (dict)
#         --['PRR']
#         --['ROR']
#   --[indi] each indication (dict)
#     --['pids'] primaryids (list)
#     --['aes'] adverse events (counter)
#     --['stats'] stats (dict)
#       --[ae] each AE (dict)
#         --['PRR']
#         --['ROR']
def getInfo(c, drugmap, indicationmap):
    start = timer()

    aeReference = scanAdverseEvents(c)
    aeMap = aeReference[0]
    aeCounter = aeReference[1]

    num_drugs = len(drugmap)
    num_indis = len(indicationmap)
    print("Searching database")
    drugcounter = 0
    info = dict()
    for drug, names in drugmap.items():
        drugcounter += 1
        print("--Drug (" + str(drugcounter) + "/" + str(num_drugs) + "):", drug)
        info[drug] = dict()
        print("  --All Indications")
        info[drug]['all'] = getDrugInfo(c, aeMap, names)
        print("    --primaryids:", len(info[drug]['all']['pids']))
        print("    --adverse events: done")
        info[drug]['all']['stats'] = getAEStats(aeCounter, info[drug]['all']['aes'])
        print("    --stats: done")
        indicounter = 0
        for indi, indi_pts in indicationmap.items():
            indicounter += 1
            print("  --Indication (" + str(indicounter) + "/" + str(num_indis) + "):", indi)
            info[drug][indi] = getDrugInfoByIndication(c, aeMap, names, indi_pts)
            print("    --primaryids:", len(info[drug][indi]['pids']))
            print("    --adverse events: done")
            info[drug][indi]['stats'] = getAEStats(aeCounter, info[drug][indi]['aes'])
            print("    --stats: done")
    end = timer()
    print("Completed in", (end-start), "seconds.")
    return info


def getDrugInfo(c, aeMap, drugnames):
    PIDs, AEs = [], Counter()
    query = sqlh.selectDrug(drugnames)
    c.execute(query)
    for i in c:
        primaryid = i[0]
        PIDs.append(primaryid)
        pid = str(primaryid)
        if pid in aeMap:
            for ae in aeMap[pid]:
                AEs[ae] += 1
    info = dict()
    info['pids'] = PIDs
    info['aes'] = AEs

    return info

def getAEStats(totalAEs, drugAEs):
    sum_totalAE = sum(totalAEs.values())
    stats = dict()
    for ae in drugAEs:
        stats[ae] = dict()
        sum_drugAE = sum(drugAEs.values())
        var_A = drugAEs[ae] # Event Y for Drug X
        var_B = sum_drugAE - var_A # Other events for Drug X
        var_C = totalAEs[ae] - var_A # Event Y for other drugs
        var_D = sum_totalAE - var_A - var_B - var_C # Other events for other drugs\
        stats[ae]['PRR'] = ss.getPRR(var_A, var_B, var_C, var_D)
        stats[ae]['ROR'] = ss.getROR(var_A, var_B, var_C, var_D)
    return stats

# Given specified drugnames / indications
# Return
#   --[pids]: List of primaryIDs for the combo of drugname / indication
#   --[aes]: Counter of 
def getDrugInfoByIndication(c, aeMap, drugnames, indications):
    PIDs, AEs = [], Counter()
    drugNameQuery = sqlh.selectDrug(drugnames)
    indicationQuery = sqlh.selectIndication(indications)
    query = drugNameQuery
    if not indicationQuery is False: query = query + " INTERSECT " + indicationQuery
    c.execute(query)
    for i in c:
        primaryid = i[0]
        PIDs.append(primaryid)
        pid = str(primaryid)
        if pid in aeMap:
            for ae in aeMap[pid]: AEs[ae] += 1
    info = dict()
    info['pids'], info['aes'] = PIDs, AEs
    return info

# Returns the following objects
# aeMap: set of preferred terms specified in each primaryid
# aeCounter: counter with frequencies of all preferred terms
def scanAdverseEvents(c):
    prog.update("Scanning adverse events", 0)
    start, aeMap, aeCounter = timer(), dict(), Counter()
    c.execute("SELECT COUNT(*) FROM reaction")
    counter, total = 0, c.fetchone()[0]
    c.execute("SELECT IFNULL(primaryid, isr), pt FROM reaction")
    for i in c:
        primaryid = str(i[0]).lower()
        pt = str(i[1]).lower().replace('\n', '')
        aeCounter[pt] += 1
        if primaryid in aeMap: aeMap[primaryid].add(pt)
        else: aeMap[primaryid] = set([ pt ]); counter += 1
        if counter%20000 == 0: prog.update("Scanning adverse events", (counter/total))
    end = timer()
    prog.update("Scanning adverse events", 1)
    print("Completed in", (end - start), "seconds.")
    return (aeMap, aeCounter)

def getFreq(reports, total):
    if reports == 0 or total == 0:
        return 0
    else:
        return float(reports) / float(total)

# Returns timestamp filename
def getOutputFilename(extension):
    timestr = time.strftime("results_%Y-%m-%d_%H%M%S")
    return (timestr + extension)
    
# count the adverse events in a specific iterable of primaryIDs
def countAdverseEvents(aeMap, primaryids):
    aeCounts = Counter()
    primaryids = set(primaryids)
    for primaryid in primaryids:
        pid = str(primaryid)
        if pid in aeMap:
            for ae in aeMap[pid]:
                aeCounts[ae] += 1
    return aeCounts

def generateReport(info):
    start = timer()
    print("Generating report")
    df_drugInfo = pd.DataFrame(columns=["Drug", "Indication", "Adverse Event", "Reports", "Frequency", "PRR", "ROR", "CI (Lower 95%)", "CI (Upper 95%)", "CI < 1"])
    df_drug = pd.DataFrame(columns=["Drug", "Indication", "Entries"])
    drugcounter = 0
    num_drugs = len(info)
    for drug, indications in info.items():
        drugcounter += 1
        msg = "--Drug (" + str(drugcounter) + "/" + str(num_drugs) + "): " + drug
        print(msg)
        total_reports = len(info[drug]['all']['pids'])
        indicounter = 0
        num_indis = len(indications)
        for indi, data in indications.items():
            indicounter += 1
            msg = "  --Indication (" + str(indicounter) + "/" + str(num_indis) + "): " + indi
            num_reports = len(info[drug][indi]['pids'])
            df_drug.loc[len(df_drug)] = [drug, indi, num_reports]
            AEs = data['aes']
            aecounter = 0
            total_AEs = sum(AEs.values())
            for ae in AEs:
                aecounter += AEs[ae]
                freq = getFreq(AEs[ae], num_reports)
                prr = data['stats'][ae]['PRR']
                ror = data['stats'][ae]['ROR']
                ci_valid = False
                try:
                    if ((ror[2]-ror[1]) < float(1)):
                        ci_valid = True
                except:
                    ci_valid = False
                df_drugInfo.loc[len(df_drugInfo)] = [drug, indi, ae, AEs[ae], freq, prr, ror[0], ror[1], ror[2], ci_valid]
                prog.update(msg, aecounter/float(total_AEs))
    filename = getOutputFilename(".xlsx")
    filename = "./output/" + filename
    print("Saving report to", filename)
    writer = pd.ExcelWriter(filename)
    df_drugInfo.to_excel(writer, "Drug Info")
    df_drug.to_excel(writer, "Drug Count")
    writer.save()
    end = timer()
    print("Completed in", (end - start), "seconds.")