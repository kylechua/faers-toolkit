from package.utils import progressbar as prog
from package.faers import dbstats as stats
from package.utils import queryhelper as qy

# Return a list of isrs which are already in FAERS
def get_crossover_duplicates(c):
    query = """SELECT DISTINCT isr
            FROM demographic WHERE case_num IN
            (SELECT DISTINCT caseid FROM demographic)"""
    c.execute(query)
    overlaps = list()
    for row in c.fetchall():
        overlaps.append(row[0])
    return overlaps

# Remove duplicates which are both in FAERS and in AERS
def remove_crossover_duplicates(c):
    overlaps = get_crossover_duplicates(c)
    return remove_duplicates(c, overlaps)

# Return a list of primaryids which are not the latest in their case
# using caseversion number
def get_FAERS_case_duplicates(c):
    return

# Remove non-recent primaryids
def remove_FAERS_case_duplicates(c):
    return

# Return a list of ISRs which are not the latest in their case
# using descending ISR number
def get_AERS_case_duplicates(c):
    return

# Remove non-recent ISRs
def remove_AERS_case_duplicates(c):
    return

# Remove reports from all tables and return the number of reports removed
def remove_duplicates(c, ids):
    initial_reports = stats.count_reports(c, 'demographic')
    queries = list()
    queries.append("""DELETE FROM demographic WHERE isr in ({0})""".format(', '.join('?' for _ in ids)))
    queries.append("""DELETE FROM drug WHERE isr in ({0})""".format(', '.join('?' for _ in ids)))
    queries.append("""DELETE FROM indication WHERE isr in ({0})""".format(', '.join('?' for _ in ids)))
    queries.append("""DELETE FROM outcome WHERE isr in ({0})""".format(', '.join('?' for _ in ids)))
    queries.append("""DELETE FROM reaction WHERE isr in ({0})""".format(', '.join('?' for _ in ids)))
    queries.append("""DELETE FROM source WHERE isr in ({0})""".format(', '.join('?' for _ in ids)))
    queries.append("""DELETE FROM therapy WHERE isr in ({0})""".format(', '.join('?' for _ in ids)))
    for query in queries:
        c.execute(query, ids)
    end_reports = stats.count_reports(c, 'demographic')
    return (initial_reports - end_reports)

# Remove duplicate entries from the database
def removeDuplicate(c):
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