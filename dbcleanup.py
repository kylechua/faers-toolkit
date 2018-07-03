import sqlite3
from package.faers import cleandb

def main():
    conn = sqlite3.connect('./db/faers-data.sqlite')
    c = conn.cursor()
    print("Connected to FAERS database.")
    initial_reports = stats.count_reports(c, 'demographic')
    print("Initial reports:", initial_reports)
    print("Removing duplicates...")
    count = cleandb.remove_crossover_duplicates(c)
    conn.commit()
    print(count, "crossover duplicates removed.")
    
    
    # Clear memory
    c.execute("VACUUM")
    conn.commit()
    conn.close()
    print("Disconnected from FAERS database.")

if __name__ == "__main__":
    main()