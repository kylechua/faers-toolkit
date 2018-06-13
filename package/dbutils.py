from collections import Counter
import numpy as np
import pandas as pd

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
    primaryids = dict()
    print("Finding primaryIDs for drugs...")
    c.execute("SELECT primaryid, drugname, prod_ai FROM drug")
    for i in c:
        primaryid = i[0]
        drugname = str(i[1]).lower()
        prod_ai = str(i[2]).lower()
        for drug, names in drugs.items():
            for name in names:
                if name in drugname or name in prod_ai:
                    if drug in primaryids:
                        primaryids[drug].add(primaryid)
                    else:
                        primaryids[drug] = set([primaryid])
    print("Done.")
    return primaryids

def drugEntryHelper(drugs, drugname, prod_ai):
    for drug, names in drugs.items():
        for name in names:
            if name in drugname or name in prod_ai:
                return True
    return False

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
    primaryids = getDrugEntries(c, drugs)
    ae = scanAdverseEvents(c)
    aeMap = ae[0]
    aeCounter = ae[1]
    df_drugAE = pd.DataFrame(columns=["Drug", "Adverse Event", "No. of Reports"])
    df_drug = pd.DataFrame(columns=["Drug", "No. of Reports"])
    df_AE = pd.DataFrame(columns=["Adverse Event", "No. of Reports"])
    for key, value in primaryids.items():
        print("Counting adverse events for", key, "from", len(value), "reports.")
        drugAEs = countAdverseEvents(aeMap, value)
        df_drug.loc[len(df_drug)] = [key, len(value)]
        for adverseEvent in drugAEs:
            df_drugAE.loc[len(df_drugAE)] = [key, adverseEvent, drugAEs[adverseEvent]]
    print("Building total adverse event table...")
    for x in aeCounter:
        key = x
        value = aeCounter[key]
        df_AE.loc[len(df_AE)] = [key, value]
    print("Done.")
    print("Saving drug information to Excel...")
    writer = pd.ExcelWriter("testresults.xlsx")
    df_drugAE.to_excel(writer, "Drug AE")
    df_drug.to_excel(writer, "Drug Info")
    df_AE.to_excel(writer, "AE Info")
    writer.save()
    print("All done!")
    
# count the adverse events in a specific iterable of primaryIDs
def countAdverseEvents(aeMap, primaryids):
    aeCounts = Counter()
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
    print("Scanning adverse events. This may take a while...")
    aeMap = dict()
    aeCounter = Counter()
    c.execute("SELECT primaryid, pt FROM react")
    for i in c:
        primaryid = str(i[0]).lower()
        pt = str(i[1]).lower().replace('\n', '')
        aeCounter[pt] += 1
        if primaryid in aeMap:
            aeMap[primaryid].add(pt)
        else:
            aeMap[primaryid] = set([ pt ])
    print("Adverse events scanned.")
    return (aeMap, aeCounter)
