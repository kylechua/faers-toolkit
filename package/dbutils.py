from collections import Counter
import numpy as np
import pandas as pd
import sys

import multiprocessing as mp
import time
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
    print("Deleting duplicates...")
    speed = 5000
    chunks = getChunks(todo, speed)
    counter = 0
    for chunk in chunks:
        basequery = " WHERE primaryid in ("
        for index, primaryid in enumerate(chunk):
            counter = counter + 1
            if index == 0:
                query = query + str(primaryid)
            else:
                query = query + ", " + str(primaryid)
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
            print(str(counter) + "/" + str(len(todo)), "cases fixed.")
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
    druglist = getMasterDrugList(drugs)
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

def worker(working_queue, output_queue):
    while True:
        if working_queue.empty() == True:
            print("empty")
            break
        else:
            picked = working_queue.get()
            print(picked[0])
            output_queue.put(picked[0])
    return

# Reverses key/values from druglist
def getMasterDrugList(drugs):
    druglist = dict()
    for drug, names in drugs.items():
        for name in names:
            druglist[name] = drug
    return druglist

#
def getDrugInfo(c, drugs):
    print("Generating drug information...")
    start = timer()
    primaryids = getDrugEntries(c, drugs)
    ae = scanAdverseEvents(c)
    aeMap = ae[0]
    aeCounter = ae[1]
    total_AEs = sum(aeCounter.values())
    df_drugAE = pd.DataFrame(columns=["Drug", "Adverse Event", "Reports", "PRR"])
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
            score_PRR = getPRR(var_A, var_B, var_C, var_D)
            df_drugAE.loc[len(df_drugAE)] = [key, adverseEvent, drugAEs[adverseEvent], score_PRR]
            counter += drugAEs[adverseEvent]
            update_progress(key, (counter/total))
    update_progress("Adding Total AEs", 0)
    total = sum(drugAEs.values())
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
    filename = "./data/" + filename
    print("Saving drug information to", filename)
    writer = pd.ExcelWriter(filename)
    df_drugAE.to_excel(writer, "Drugs and AE")
    df_drug.to_excel(writer, "Drug Totals")
    df_AE.to_excel(writer, "AE Totals")
    writer.save()
    end = timer()
    print("All done! This program took", (end - start), "seconds.")

def getPRR(a, b, c, d):
    if a == 0 or b == 0 or c == 0 or d == 0:
        return 0
    else:
        return (a/float(a+b)) / (c/float(c+d))

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
    update_progress("Scanning adverse events", 0)
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
            update_progress("Scanning adverse events", (counter/total))
    end = timer()
    update_progress("Scanning adverse events", 1)
    print("Adverse events scanned in", (end - start), "seconds.")
    return (aeMap, aeCounter)

def update_progress(message, progress):
    barLength = 30 # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "\x1b[6;37;42mFinished!\x1b[0m\r\n"
    block = int(round(barLength*progress))
    text = ("\r" + message + ": {0} {1}% {2}").format( "\x1b[1;35;44m" + " "*block + "\x1b[0m" + "\x1b[0;36;47m" + " "*(barLength-block) + "\x1b[0m", str(progress*100)[:4], status)
    sys.stdout.write(text)
    sys.stdout.flush()
