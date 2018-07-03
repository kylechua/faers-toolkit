"""SQLite module used to assit FAERS parse.py module

These functions are separated from parse.py to keep the DB modular. Feel free
to write your own for your own db.
"""

import pymysql

from os.path import isfile
from os import remove
import zipfile
import zlib # Needed for zipfile compression type (not all systems may have this)


conn = pymysql.connect(host='localhost', user='username', password='password', db='FAERS')
cur = conn.cursor()

def setupDB():
    """
    setupDB() createds SQLite tables using the global connection.
    """
    cur.execute("""
    create table DEMOGRAPHIC (ISR integer, PRIMARYID integer, CASEID integer, CASEVERSION integer, CASE_NUM integer, I_F_COD text,
                    FOLL_SEQ text, IMAGE text, EVENT_DT text, MFR_DT text, INIT_FDA_DT text, FDA_DT text,
                    REPT_COD text, AUTH_NUM integer, MFR_NUM text, MFR_SNDR text, LIT_REF text, AGE varchar, AGE_COD text, AGE_GRP text, SEX text,
                    GNDR_COD text, E_SUB text, WT varchar, WT_COD text, REPT_DT text,
                    OCCP_COD text, DEATH_DT text, TO_MFR text, CONFID text, REPORTER_COUNTRY text, OCCR_COUNTRY text)
    """)
    cur.execute("""
    create table DRUG (ISR integer, PRIMARYID integer, CASEID integer, DRUG_SEQ integer, ROLE_COD text,
                    DRUGNAME text, PROD_AI text, VAL_VBM integer, ROUTE text, DOSE_VBM text, CUM_DOSE_CHR text, CUM_DOSE_UNIT text, DECHAL text,
                    RECHAL text, LOT_NUM text, EXP_DT text, NDA_NUM text, DOSE_AMT varchar, DOSE_UNIT text, DOSE_FORM text, DOSE_FREQ text)
    """)
    cur.execute("""
    create table REACTION (ISR integer, PRIMARYID integer, CASEID integer, PT text not null, DRUG_REC_ACT text)
    """)
    cur.execute("""
    create table OUTCOME (ISR integer, PRIMARYID integer, CASEID integer, OUTC_COD text not null)
    """)
    cur.execute("""
    create table SOURCE (ISR integer, PRIMARYID integer, CASEID integer, RPSR_COD text not null)
    """)
    cur.execute("""
    create table THERAPY (ISR integer, PRIMARYID integer, CASEID integer, DRUG_SEQ integer, START_DT text,
                    END_DT text, DUR varchar, DUR_COD text)
    """)
    cur.execute("""
    create table INDICATION (ISR integer, PRIMARYID integer, CASEID integer, DRUG_SEQ integer, INDI_DRUG_SEQ integer, INDI_PT text)
    """)
    conn.commit()

def getStatement(table_name, field_names):
    fs = ' (' + ','.join(field_names) + ')'
    qs = ['%s'] * len(field_names)
    stm = 'INSERT INTO ' + table_name + fs + ' VALUES(' + ', '.join(qs) + ')'
    return stm

def writeEntry(query, values):
    print("Writing", len(values), "entries.")
    cur.executemany(query, values)
    conn.commit()

def preClose():
    cur.execute('VACUUM')


def closeDB():
    """
    closeDB() commits and closes the Db connection.
    """
    conn.commit()
    conn.close()


def postClose():
    print("Done.")
