import sqlite3

def main():
    conn = sqlite3.connect('./data/db/faers-data.sqlite')
    c = conn.cursor()
    print("Connected to FAERS database.")
    conn.close()
    print("Disconnected from FAERS database.")

if __name__ == "__main__":
    main()