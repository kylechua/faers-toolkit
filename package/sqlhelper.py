# Find PIDs from list of drug names
def selectDrug(drugnames):
    query = "SELECT primaryid "
    query += "FROM drug WHERE ("
    first = True
    for drugname in drugnames:
        if not first:
            query += " OR "
        else:
            first = False
        query += "drug.drugname Like '%" + drugname + "%'"
        query += " OR drug.prod_ai Like '%" + drugname + "%'"
    query += ")"
    return query

# Find PIDs from list of indications
def selectIndication(indications):
    query = "SELECT primaryid "
    query += "FROM indication WHERE ("
    first = True
    for indi in indications:
        if not first:
            query += " OR "
        else:
            first = False
        query += "indication.indi_pt Like '%" + indi + "%'"
    query += ")"
    return query