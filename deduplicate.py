import sqlite3
from package.faers import cleandb
from package.faers import dbstats as stats

def main():
    conn = sqlite3.connect('./db/faers-data.sqlite')
    c = conn.cursor()
    print("Connected to FAERS database.")

    initial_reports = stats.count_reports(c, 'demographic')
    print("Initial reports:", initial_reports)

    print("Step (1/3) - Removing crossover duplicates...")
    count = cleandb.remove_crossover_duplicates(c)
    print("--", count, "crossover duplicates removed.")

    print("Step (2/3) - Removing FAERS case duplicates...")
    count = cleandb.remove_FAERS_case_duplicates(c)
    print("--", count, "FAERS case duplicates removed.")

    print("Step (3/x) - Removing AERS case duplicates...")
    count = cleandb.remove_AERS_case_duplicates(c)
    print("--", count, "AERS case duplicates removed.")

    end_reports = stats.count_reports(c, 'demographic')
    print("End reports:", end_reports)
    print("Total reports removed:", (initial_reports - end_reports))

    # Clear memory
    c.execute("VACUUM")
    conn.commit()

    conn.close()
    print("Disconnected from FAERS database.")

if __name__ == "__main__":
    main()