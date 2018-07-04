from package.utils import progressbar as prog
from package.faers import dbstats as stats
from package.utils import chunks

# Return a list of isrs which are already in FAERS
def get_crossover_duplicates(c):
    query = """SELECT DISTINCT isr
            FROM demographic WHERE case_num IN
            (SELECT DISTINCT caseid FROM demographic)"""
    print("-- finding crossover duplicates")
    c.execute(query)
    overlaps = list()
    for row in c:
        overlaps.append(row[0])
    return overlaps

# Remove duplicates which are both in FAERS and in AERS
def remove_crossover_duplicates(c):
    initial_reports = stats.count_reports(c, 'demographic')
    overlaps = get_crossover_duplicates(c)
    chunk = overlaps
    print("-- deleting from all tables")
    queries = list()
    queries.append("""DELETE FROM demographic WHERE isr in ({0})""".format(', '.join('?' for _ in chunk)))
    queries.append("""DELETE FROM drug WHERE isr in ({0})""".format(', '.join('?' for _ in chunk)))
    queries.append("""DELETE FROM indication WHERE isr in ({0})""".format(', '.join('?' for _ in chunk)))
    queries.append("""DELETE FROM outcome WHERE isr in ({0})""".format(', '.join('?' for _ in chunk)))
    queries.append("""DELETE FROM reaction WHERE isr in ({0})""".format(', '.join('?' for _ in chunk)))
    queries.append("""DELETE FROM source WHERE isr in ({0})""".format(', '.join('?' for _ in chunk)))
    queries.append("""DELETE FROM therapy WHERE isr in ({0})""".format(', '.join('?' for _ in chunk)))
    for query in queries:
        c.execute(query, chunk)
    c.execute("COMMIT")
    end_reports = stats.count_reports(c, 'demographic')
    return (initial_reports - end_reports)

# Delete primaryids which are not the latest in their case
# using caseversion number
def delete_FAERS_case_duplicates(c, table):
    query = "DELETE FROM " + table + """ WHERE primaryid NOT IN (
                SELECT d.primaryid 
                FROM (SELECT caseid, MAX(caseversion) AS max_case 
                    FROM demographic
                    GROUP BY caseid) AS c
                INNER JOIN demographic AS d
                    ON d.caseid = c.caseid
                    AND d.caseversion = c.max_case
                    AND d.caseversion NOT NULL
            )"""
    print("-- deleting from", table)
    c.execute(query)
    c.execute("COMMIT")
    return

# Remove non-recent primaryids
def remove_FAERS_case_duplicates(c):
    initial_reports = stats.count_reports(c, 'demographic')
    tables = stats.get_tables()
    for table in tables:
        delete_FAERS_case_duplicates(c, table)
    end_reports = stats.count_reports(c, 'demographic')
    return (initial_reports - end_reports)

# Return a list of ISRs which are not the latest in their case
# using descending ISR number
def delete_AERS_case_duplicates(c, table):
    query = "DELETE FROM " + table + """ WHERE isr NOT IN (
                SELECT d.isr
                FROM (SELECT case_num, MAX(isr) AS max_isr
                    FROM demographic
                    GROUP BY case_num) AS c
                INNER JOIN demographic AS d
                    ON d.case_num = c.case_num
                    AND d.isr = c.max_isr
                    AND d.isr NOT NULL
            )"""
    print("-- deleting from", table)
    c.execute(query)
    c.execute("COMMIT")
    return

# Remove non-recent ISRs
def remove_AERS_case_duplicates(c):
    initial_reports = stats.count_reports(c, 'demographic')
    tables = stats.get_tables()
    for table in tables:
        delete_AERS_case_duplicates(c, table)
    end_reports = stats.count_reports(c, 'demographic')
    return (initial_reports - end_reports)
