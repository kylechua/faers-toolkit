def count_reports(c, table):
    query = """SELECT COUNT(*) FROM (SELECT DISTINCT primaryid, isr FROM {0})""".format(table)
    c.execute(query)
    return c.fetchall()[0][0]

def count_cases(c, table):
    query = """SELECT COUNT(*) FROM (SELECT DISTINCT caseid, case_num FROM {0})""".format(table)
    c.execute(query)
    return c.fetchall()[0][0]

def get_tables():
    return ["indication", "drug", "reaction", "source", "therapy", "outcome", "demographic"]

