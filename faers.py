import sqlite3, csv
from package import dbutils as DBHelper

def main():
    conn = sqlite3.connect('./data/db/faers-data.sqlite')
    c = conn.cursor()
    print("Connected to FAERS database.")
    # -------------------
    # YOUR CODE GOES HERE
    # -------BEGIN-------

    drugs = parseFile('./data/input/immunotherapy.csv')
    indications = parseFile('./data/input/indications.csv')
    #DBHelper.getDrugAEInfo(c, drugs)
    DBHelper.getDrugPIDsByIndication(c, ["Nivolumab", "Opdivo"], ["Malignant melanoma"])

    
    # REMEMBER TO ADD ALL DRUGS AS A CATEGORY

    # -------END---------
    conn.close()
    print("Disconnected from FAERS database.")

def parseFile(file):
    res = dict()
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            name = row[0]
            res[name] = set()
            for alias in row:
                res[name].add(alias.lower())
    print("Parsed", file)
    return res

if __name__ == "__main__":
    main()