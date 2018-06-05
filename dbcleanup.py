import sqlite3
from package import dbutils as DBHelper

def main():
    conn = sqlite3.connect('./data/db/faers-data.sqlite')
    c = conn.cursor()
    print("Connected to FAERS database.")
    
    DBHelper.removeDuplicates(c)
    c.execute("VACUUM")

    conn.close()
    print("Disconnected from FAERS database.")

if __name__ == "__main__":
    main()