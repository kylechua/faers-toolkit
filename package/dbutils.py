from collections import Counter
import numpy as np
import pandas as pd
import sys
import cmath

import multiprocessing as mp
import time

from package import sqlhelper as sqlh
from package import progressbar as prog
from timeit import default_timer as timer

# Returns a set of unique primaryIDs in database
def getEntries(c):
    c.execute("SELECT primaryid FROM drug")
    primaryids = set()
    for i in c:
        primaryids.add(i[0])
    return primaryids

# Remove duplicate entries from the database
def removeDuplicates(c):
    print("Removing duplicates from the database. This may take a while...")
    # Initial entry tally
    initialCount = len(getEntries(c))
    print("Initial entries:", initialCount)
    # Map primaryids to their associated caseids
    print("Scanning case information...")
    c.execute("SELECT primaryid, caseid FROM drug")
    index = dict()
    for i in c:
        caseid = i[1]
        primaryid = i[0]
        if caseid in index:
            index[caseid].add(primaryid)
        else:
            index[caseid] = set([primaryid])
    # Determine which entries are duplicates
    print("Finding duplicate entries...")
    todo = []
    for case, primaries in index.items():
        versions = dict()
        if len(primaries) > 1:
            for primaryid in primaries:
                if not primaryid in versions:
                    versions[primaryid] = []
                version = str(primaryid).replace(str(case), '')
                versions[primaryid].append(version)
            latest = max(versions, key=versions.get)
            versions.pop(latest, None)
            for key in list(versions.keys()):
                todo.append(key)
    print("Duplicates found:", len(todo))
    # Delete duplicates from all database tables
    update_progress("Deleting duplicates", 0)
    speed = 5000
    chunks = getChunks(todo, speed)
    counter = 0
    for chunk in chunks:
        basequery = " WHERE primaryid in ("
        for index, primaryid in enumerate(chunk):
            counter = counter + 1
            if index == 0:
                basequery = basequery + str(primaryid)
                update_progress("Deleting duplicates", (counter/float(len(todo))))
            else:
                basequery = basequery + ", " + str(primaryid)
        basequery = basequery + ")"
        queries = []
        queries.append("DELETE FROM drug" + basequery)
        queries.append("DELETE FROM demo" + basequery)
        queries.append("DELETE FROM react" + basequery)
        queries.append("DELETE FROM outcome" + basequery)
        queries.append("DELETE FROM source" + basequery)
        queries.append("DELETE FROM therapy" + basequery)
        queries.append("DELETE FROM indication" + basequery)
        for query in queries:
            c.execute(query)
            c.execute("COMMIT")
    # Final entry tally
    endCount = len(getEntries(c))
    deleted = initialCount - endCount
    print("Initial entries:", initialCount)
    print("Final entries:", endCount)
    print(deleted, "entries deleted.")

# Given a list of unspecified size, return it as list of lists of n size
def getChunks(l, n):
    for i in range(0, len(l), n): 
        yield l[i:i + n]

# Returns a map of primaryIDs related to each drug
# Key = drug
# Value = set of related primaryIDs
def getDrugEntries(c, drugs):
    update_progress("Finding drug entries", 0)
    start = timer()
    primaryids = dict()
    druglist = getReverseDrugList(drugs)
    drugset = set()
    for key in druglist.keys():
        drugset.add(key)
    counter = 0
    c.execute("SELECT COUNT(*) FROM drug")
    total = c.fetchone()[0]
    c.execute("SELECT primaryid, drugname, prod_ai FROM drug")
    for i in c:
        primaryid = i[0]
        drugname = str(i[1]).lower()
        prod_ai = str(i[2]).lower()
        currName = drugname + " " + prod_ai
        if any(substring in currName for substring in drugset):
            for name in drugset:
                if name in currName:
                    drug = druglist[name]
                    if drug in primaryids:
                        primaryids[drug].add(primaryid)
                    else:
                        primaryids[drug] = set([primaryid])
        counter+=1
        if counter%20000 == 0:
            update_progress("Finding drug entries", (counter/total))
    end = timer()
    update_progress("Finding drug entries", 1)
    print("Drug entries found in", (end - start), "seconds.")
    return primaryids

# info
# --[*drugname] drug (dict)
#   --[all] all indications (dict)
#     --[pids] primaryids (list)
#     --[aes] adverse events (counter)
#     --[stats] stats (dict)
#   --[*indiname] each indication (dict)
#     --[pids] primaryids (list)
#     --[aes] adverse events (counter)
#     --[stats] stats (dict)
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
        print("--Drug (" + str(drugcounter) + "/" + str(num_drugs) + "):", drug)
        drugcounter += 1
        info[drug] = dict()
        print("  --All Indications")
        info[drug]['all'] = getDrugInfo(c, aeMap, names)
        print("    --primaryids:", len(info[drug]['all']['pids']))
        print("    --adverse events: done")
        info[drug]['all']['stats'] = getAEStats(aeCounter, info[drug]['all']['adverse events'])
        indicounter = 0
        for indi, indi_pts in indicationmap.items():
            indicounter += 1
            print("  --Indication (" + str(indicounter) + "/" + str(num_indis) + "):", indi)
            info[drug][indi] = getDrugInfoByIndication(c, aeMap, names, indi_pts)
            print("    --primaryids:", len(info[drug][indi]['pids']))
            print("    --adverse events: done")
            info[drug][indi]['stats'] = getAEStats(aeCounter, info[drug][indi]['adverse events'])
    end = timer()
    print("Done. Took", (end-start), "seconds.")

    print(info)

def getAEStats(totalAEs, drugAEs):
    stats = dict()
    sum_totalAE = sum(totalAEs.values())
    for ae in drugAEs:
        sum_drugAE = sum(drugAEs.values())
        var_A = drugAEs[ae] # Event Y for Drug X
        var_B = sum_drugAE - var_A # Other events for Drug X
        var_C = totalAEs[ae] - var_A # Event Y for other drugs
        var_D = sum_totalAE - var_A - var_B - var_C # Other events for other drugs


# Given specified drugnames / indications
# Return
#   --[pids]: List of primaryIDs for the combo of drugname / indication
#   --[aes]: Counter of 
def getDrugInfoByIndication(c, aeMap, drugnames, indications):
    PIDs = []
    AEs = Counter()

    drugNameQuery = sqlh.selectDrug(drugnames)
    indicationQuery = sqlh.selectIndication(indications)
    query = drugNameQuery + " INTERSECT " + indicationQuery
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

def getDrugInfo(c, aeMap, drugnames):
    PIDs = []
    AEs = Counter()

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

# Parameter: Drug to get names from, Map of drug names
# Returns: List of drug names for given drug
def getDrugNameList(drug, drugmap):
    return 

# Reverses key/values from druglist for faster lookup
def getReverseDrugList(drugs):
    druglist = dict()
    for drug, names in drugs.items():
        for name in names:
            druglist[name] = drug
    return druglist

#
def getDrugAEInfo(c, drugs):
    print("Generating drug information...")
    start = timer()
    primaryids = getDrugEntries(c, drugs)
    ae = scanAdverseEvents(c)
    aeMap = ae[0]
    aeCounter = ae[1]
    total_AEs = sum(aeCounter.values())
    df_drugAE = pd.DataFrame(columns=["Drug", "Adverse Event", "Reports", "Frequency", "PRR", "ROR", "CI (Lower 95%)", "CI (Upper 95%)"])
    df_drug = pd.DataFrame(columns=["Drug", "No. of Total Reports"])
    df_AE = pd.DataFrame(columns=["Adverse Event", "No. of Total Reports"])
    for key, value in primaryids.items():
        print("Adding", len(value), "reports for:", key)
        update_progress(key, 0)
        # Retrieve a Counter for each adverse event for the given drug (key)
        drugAEs = countAdverseEvents(aeMap, value)
        df_drug.loc[len(df_drug)] = [key, len(value)]
        total = sum(drugAEs.values())
        counter = 0
        for adverseEvent in drugAEs:
            # Get PRR
            var_A = drugAEs[adverseEvent] # Event Y for Drug X
            var_B = total - var_A # Other events for Drug X
            var_C = aeCounter[adverseEvent] - var_A # Event Y for other drugs
            var_D = total_AEs - var_A - var_B - var_C # Other events for other drugs
            score_Freq = getFreq(drugAEs[adverseEvent], len(value))
            score_PRR = getPRR(var_A, var_B, var_C, var_D)
            score_ROR = getROR(var_A, var_B, var_C, var_D)
            df_drugAE.loc[len(df_drugAE)] = [key, adverseEvent, drugAEs[adverseEvent], score_Freq, score_PRR, score_ROR[0], score_ROR[1], score_ROR[2]]
            counter += drugAEs[adverseEvent]
            update_progress(key, (counter/total))
    update_progress("Adding Total AEs", 0)
    total = sum(aeCounter.values())
    counter = 0
    for x in aeCounter:
        key = x
        value = aeCounter[key]
        df_AE.loc[len(df_AE)] = [key, value]
        counter += 1
        if counter%100 == 0:
            update_progress("Adding Total AEs", (counter/total))
    update_progress("Adding Total AEs", 1)
    filename = getOutputFilename(".xlsx")
    filename = "./data/drug-ae_" + filename
    print("Saving drug information to", filename)
    writer = pd.ExcelWriter(filename)
    df_drugAE.to_excel(writer, "Drugs and AE")
    df_drug.to_excel(writer, "Drug Totals")
    df_AE.to_excel(writer, "AE Totals")
    writer.save()
    end = timer()
    print("All done! This program took", (end - start), "seconds.")

def getDrugOutcomeInfo(c, drugs):
    print("Generating drug information...")
    start = timer()
    primaryids = getDrugEntries(c, drugs)
    oc = scanOutcomes(c)
    outcomeMap = oc[0]
    outcomeCounter = oc[1]
    total_OCs = sum(outcomeCounter.values())
    df_drugOC = pd.DataFrame(columns=["Drug", "Outcome", "Reports", "PRR"])
    df_drug = pd.DataFrame(columns=["Drug", "No. of Total Reports"])
    df_OC = pd.DataFrame(columns=["Outcome", "No. of Total Reports"])
    for key, value in primaryids.items():
        print("Adding", len(value), "reports for:", key)
        update_progress(key, 0)
        # Retrieve a Counter for each adverse event for the given drug (key)
        drugOCs = countAdverseEvents(outcomeMap, value)
        df_drug.loc[len(df_drug)] = [key, len(value)]
        total = sum(drugOCs.values())
        counter = 0
        for outcome in drugOCs:
            # Get PRR
            var_A = drugOCs[outcome] # Event Y for Drug X
            var_B = total - var_A # Other events for Drug X
            var_C = outcomeCounter[outcome] - var_A # Event Y for other drugs
            var_D = total_OCs - var_A - var_B - var_C # Other events for other drugs
            score_PRR = getPRR(var_A, var_B, var_C, var_D)
            df_drugOC.loc[len(df_drugOC)] = [key, outcome, drugOCs[outcome], score_PRR]
            counter += drugOCs[outcome]
            update_progress(key, (counter/total))
    update_progress("Adding Total OCs", 0)
    total = sum(drugOCs.values())
    counter = 0
    for x in outcomeCounter:
        key = x
        value = outcomeCounter[key]
        df_OC.loc[len(df_OC)] = [key, value]
        counter += 1
        if counter%100 == 0:
            update_progress("Adding Total OCs", (counter/total))
    update_progress("Adding Total OCs", 1)
    filename = getOutputFilename(".xlsx")
    filename = "./data/drug-oc_" + filename
    print("Saving drug information to", filename)
    writer = pd.ExcelWriter(filename)
    df_drugOC.to_excel(writer, "Drugs and OC")
    df_drug.to_excel(writer, "Drug Totals")
    df_OC.to_excel(writer, "OC Totals")
    writer.save()
    end = timer()
    print("All done! This program took", (end - start), "seconds.")

def getPRR(a, b, c, d):
    if a == 0 or b == 0 or c == 0 or d == 0:
        return 0
    else:
        return (a/float(a+b)) / (c/float(c+d))

def getROR(a, b, c, d):
    if a == 0 or b == 0 or c == 0 or d == 0:
        return [0, 0, 0]
    else:
        ROR = (a/float(c)) / (b/float(d))
        UpperCI = cmath.exp( cmath.log(ROR) + 1.96*cmath.sqrt( 1/float(a) + 1/float(b) + 1/float(c) + 1/float(d) ) )
        LowerCI = cmath.exp( cmath.log(ROR) - 1.96*cmath.sqrt( 1/float(a) + 1/float(b) + 1/float(c) + 1/float(d) ) )
        return [ROR, LowerCI, UpperCI]

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

# Returns the following objects
# aeMap: set of preferred terms specified in each primaryid
# aeCounter: counter with frequencies of all preferred terms
def scanAdverseEvents(c):
    prog.update("Scanning adverse events", 0)
    start = timer()
    aeMap = dict()
    aeCounter = Counter()
    c.execute("SELECT COUNT(*) FROM react")
    total = c.fetchone()[0]
    counter = 0
    c.execute("SELECT primaryid, pt FROM react")
    for i in c:
        primaryid = str(i[0]).lower()
        pt = str(i[1]).lower().replace('\n', '')
        aeCounter[pt] += 1
        if primaryid in aeMap:
            aeMap[primaryid].add(pt)
        else:
            aeMap[primaryid] = set([ pt ])
        counter += 1
        if counter%20000 == 0:
            prog.update("Scanning adverse events", (counter/total))
    end = timer()
    prog.update("Scanning adverse events", 1)
    print("Completed in", (end - start), "seconds.")
    return (aeMap, aeCounter)

# Returns the following objects
# outcomeMap: set of preferred terms specified in each primaryid
# outcomeCounter: counter with frequencies of all outcomes
def scanOutcomes(c):
    update_progress("Scanning outcomes", 0)
    start = timer()
    outcomeMap = dict()
    outcomeCounter = Counter()
    c.execute("SELECT COUNT(*) FROM outcome")
    total = c.fetchone()[0]
    counter = 0
    c.execute("SELECT primaryid, outc_cod FROM outcome")
    for i in c:
        primaryid = str(i[0]).lower()
        oc = str(i[1]).lower().replace('\n', '')
        outcomeCounter[oc] += 1
        if primaryid in outcomeMap:
            outcomeMap[primaryid].add(oc)
        else:
            outcomeMap[primaryid] = set([ oc ])
        counter += 1
        if counter%20000 == 0:
            update_progress("Scanning outcomes", (counter/total))
    end = timer()
    update_progress("Scanning outcomes", 1)
    print("Outcomes scanned in", (end - start), "seconds.")
    return (outcomeMap, outcomeCounter)
