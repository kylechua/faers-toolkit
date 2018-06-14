import sqlite3, csv
from package import dbutils as DBHelper

def main():
    conn = sqlite3.connect('./data/db/faers-data.sqlite')
    c = conn.cursor()
    print("Connected to FAERS database.")
    # -------------------
    # YOUR CODE GOES HERE
    # -------BEGIN-------

    drugs = parseDrugList('./data/druglist.csv')
    DBHelper.getDrugInfo(c, drugs)
    

    # -------END---------
    conn.close()
    print("Disconnected from FAERS database.")

def parseDrugList(file):
    drugs = dict()
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            name = row[0]
            drugs[name] = set()
            for alias in row:
                drugs[name].add(alias.lower())
    print("Parsed drug list.")
    return drugs


if __name__ == "__main__":
    main()