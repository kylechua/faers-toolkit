"""Parse module for parsing the FDA's FAERS "$" delimitted files.
This module expects the first argument passed to it to be the database type
which is specified by module name.
"""


from os import listdir
from os.path import join, isfile
from sys import argv
import io
import zipfile
import re
import importlib
import functools

# Import DB module and create DB structure
db_selection = 'sqlite'
if len(argv) > 1:
    db_selection = argv[1]
db = importlib.import_module('.'+ db_selection, 'package.parser_modules')
db.setupDB()


# The FDA changes their data structure and ordering between releases.
class DBfields():

    def __init__(self, year, quarter):
        self.yearq = year + 0.1 * quarter
        self.trans = {
            'THER': ['therapy', self.therapy_fields],
            'RPSR': ['source', self.source_fields],
            'REAC': ['reaction', self.react_fields],
            'OUTC': ['outcome', self.outcome_fields],
            'INDI': ['indication', self.indication_fields],
            'DRUG': ['drug', self.drug_fields],
            'DEMO': ['demographic', self.demo_fields],
        }

    def translate(self, first_four):
        table_name = self.trans[first_four][0]
        table_fields = self.trans[first_four][1]()
        return {'table_name': table_name, 'table_fields': table_fields}

    def therapy_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'DRUG_SEQ', 'START_DT', 'END_DT', 'DUR', 'DUR_COD']
        else:
            return ['PRIMARYID', 'CASEID', 'DRUG_SEQ', 'START_DT', 'END_DT', 'DUR', 'DUR_COD']

    def source_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'RPSR_COD']
        else:
            return ['PRIMARYID', 'CASEID', 'RPSR_COD']

    def react_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'PT']
        elif self.yearq < 2014.3:
            return ['PRIMARYID', 'CASEID', 'PT']
        else:
            return ['PRIMARYID', 'CASEID', 'PT', 'DRUG_REC_ACT']

    def outcome_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'OUTC_COD']
        else:
            return ['PRIMARYID', 'CASEID', 'OUTC_COD']

    def indication_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'DRUG_SEQ', 'INDI_PT']
        else:
            return ['PRIMARYID', 'CASEID', 'INDI_DRUG_SEQ', 'INDI_PT']

    def drug_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'DRUG_SEQ', 'ROLE_COD', 'DRUGNAME', 'VAL_VBM', 'ROUTE', 'DOSE_VBM', 'DECHAL', 'RECHAL', 'LOT_NUM', 'EXP_DT', 'NDA_NUM']
        elif self.yearq < 2014.3:
            return ['PRIMARYID', 'CASEID', 'DRUG_SEQ', 'ROLE_COD', 'DRUGNAME', 'VAL_VBM', 'ROUTE', 'DOSE_VBM', 'CUM_DOSE_CHR', 'CUM_DOSE_UNIT',
                    'DECHAL', 'RECHAL', 'LOT_NUM', 'EXP_DT', 'NDA_NUM', 'DOSE_AMT', 'DOSE_UNIT', 'DOSE_FORM', 'DOSE_FREQ']
        else:
            return ['PRIMARYID', 'CASEID', 'DRUG_SEQ', 'ROLE_COD', 'DRUGNAME', 'PROD_AI', 'VAL_VBM', 'ROUTE', 'DOSE_VBM', 'CUM_DOSE_CHR', 'CUM_DOSE_UNIT',
                    'DECHAL', 'RECHAL', 'LOT_NUM', 'EXP_DT', 'NDA_NUM', 'DOSE_AMT', 'DOSE_UNIT', 'DOSE_FORM', 'DOSE_FREQ']

    def demo_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'CASE_NUM', 'I_F_COD', 'FOLL_SEQ', 'IMAGE', 'EVENT_DT', 'MFR_DT', 'FDA_DT', 'REPT_COD', 'MFR_NUM', 'MFR_SNDR', 'AGE', 'AGE_COD',
                    'GNDR_COD', 'E_SUB', 'WT', 'WT_COD', 'REPT_DT', 'OCCP_COD', 'DEATH_DT', 'TO_MFR', 'CONFID', 'REPORTER_COUNTRY']
        elif self.yearq < 2014.3:
            return ['PRIMARYID', 'CASEID', 'CASEVERSION', 'I_F_COD', 'EVENT_DT', 'MFR_DT', 'INIT_FDA_DT', 'FDA_DT', 'REPT_COD', 'MFR_NUM', 'MFR_SNDR',
                    'AGE', 'AGE_COD', 'GNDR_COD', 'E_SUB', 'WT', 'WT_COD', 'REPT_DT', 'TO_MFR', 'OCCP_COD', 'REPORTER_COUNTRY', 'OCCR_COUNTRY']
        else:
            return ['PRIMARYID', 'CASEID', 'CASEVERSION', 'I_F_COD', 'EVENT_DT', 'MFR_DT', 'INIT_FDA_DT', 'FDA_DT', 'REPT_COD', 'AUTH_NUM', 'MFR_NUM',
                    'MFR_SNDR', 'LIT_REF', 'AGE', 'AGE_COD', 'AGE_GRP', 'SEX', 'E_SUB', 'WT', 'WT_COD', 'REPT_DT', 'TO_MFR', 'OCCP_COD', 'REPORTER_COUNTRY', 'OCCR_COUNTRY']


tbl_count = {}

files = ['data/' + f for f in listdir('data') if isfile(join('data', f)) and f[-4:].lower() == '.zip']

for filename in files:
    if not zipfile.is_zipfile(filename):
        raise Exception(filename + ' is not a zip file')

# Gather all the zipfiles and make sure we can get the year and quarter from
# each file name.
valid_files = []
ascii_file_re = re.compile(r'as(?:c*)i(?:i*)/(.+?)\.txt', re.I)
ascii_year_re = re.compile(r'\D(\d{4})\D')
ascii_quarter_re = re.compile(r'q(\d)\.', re.I)
for filename in files:
    try:
        year = int(ascii_year_re.search(filename).group(1))
        quarter = int(ascii_quarter_re.search(filename).group(1))
    except:
        raise Exception('Unable to ascertain date information for file ' + filename)
    try:
        f = zipfile.ZipFile(filename, 'r')
        f.infolist()
    except:
        raise Exception('Unable to read ' + filename)
    for name in f.namelist():
        if ascii_file_re.match(name):
            valid_files.append([filename, name, year, quarter])


# Sort the files from past to present
def sort_files(a, b):
    if a[2] != b[2]:
        return a[2] - b[2]
    return a[3] - b[3]
valid_files.sort(key=functools.cmp_to_key(sort_files))


def pop_newlines(fields, req_fields):
    while len(fields) > req_fields and (fields[-1] == "\r\n" or fields[-1] == "" or fields[-1] == "\n"):
        fields.pop()

# Verify every field in the files to make sure they add up.  These records are
# likely manually entered and need to be cleaned.
for zip_files in valid_files:
    # Unpack list of info
    zip_filename = zip_files[0]
    filename = zip_files[1]
    year = zip_files[2]
    quarter = zip_files[3]

    zip_name = ascii_file_re.search(zip_files[1]).group(1)
    if zip_name[:4] == 'STAT':
        continue  # STAT is not $ delimited and likely wrong after scrubbing

    trans = DBfields(year, quarter).translate(zip_name[:4].upper())
    table_name = trans['table_name']

    # if (table_name == 'drug') or (table_name == 'reaction') or (table_name=='demographic'):
    #     print('')
    # else:
    #     continue

    print(zip_filename + ' ' + filename + ' (' + trans['table_name'] + ')...',)
    f = zipfile.ZipFile(zip_filename, 'r')
    h = f.open(filename, 'r')

    lines = h.readlines()
    total_lines = len(lines)
    req_fields = len(trans['table_fields'])
    fields_obj = DBfields(year, quarter)

    query = db.getStatement(table_name, trans['table_fields'])
    fields_list = []
    i = 0
    while i < total_lines:
        # Skip the first line since it is only headers
        if i == 0:
            i += 1
            continue

        l = lines[i].decode('utf-8')
        fields = l.split('$')
        fields[len(fields)-1] = fields[len(fields)-1].replace('\n', '').replace('\r','')

        # Try to concat the next lines if the field count doesn't add up
        extra_lines = 0
        while len(fields) < req_fields and i + 1 + extra_lines < total_lines:
            extra_lines += 1
            l += lines[i + extra_lines].decode('utf-8')
            fields = l.split('$')
            # Check if we went over the field count and give up
            pop_newlines(fields, req_fields)
            if len(fields) > req_fields:
                print("\t", zip_files[1], i+1, len(fields), req_fields)
                fields = lines[i].split('$')
                extra_lines = 0
                break

        # Some files have extra blank fields
        pop_newlines(fields, req_fields)

        field_count = len(fields)
        if field_count == req_fields:
            # TODO remove all newline characters from entries
            try:
                if table_name not in tbl_count:
                    tbl_count[table_name] = {'records': 0, 'files': 0}
                tbl_count[table_name]['records'] += 1
                for index, f in enumerate(fields):
                    f = f.strip()
                    if (len(f) == 0):
                        fields[index] = None
                fields_list.append(fields)
            except Exception as e:
                print(l)
                print(fields)
                raise Exception(e)
        else:
            # FDA probably forgot to escape a $
            print("\t", trans['table_name'], ' - skipping line ', i+1, year, quarter, len(fields), req_fields)
            print("\t\t", fields)

        # Last line, add to DB
        if i == (total_lines-1):
            db.writeEntry(query, fields_list)

        i += 1 + extra_lines
    tbl_count[table_name]['files'] += 1

db.preClose()
db.closeDB()
db.postClose()
print(tbl_count)
