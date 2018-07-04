FAERS Toolkit
=============
This repository provides tools for data analysis from the [FDA Adverse Event Reporting System **(FAERS)**](https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/). The toolkit uses FAERS data which is [publicly available online](https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm). It is able to parse FAERS (and older AERS) data into a MySQL or sqlite database and analyze it using a parsing module adapted from [wizzl35's script](https://github.com/wizzl35/faers-data). I am not responsible for any conclusions drawn from the data, nor do I guarantee its efficacy.

### What can FAERS Toolkit do?
* [Parse AERS/FAERS data into sqlite or MySQL database](#parsing-faers-data)
* [Remove duplicate entries from data](#removing-duplicate-entries)
* Calculate signal scores (under construction)

## Using FAERS Toolkit
### Parsing FAERS data
To parse FAERS (and AERS) data into a relational database (by default it is sqlite), [download the ASCII zip file](https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/) for each quarter of your database and store them in the data folder (e.g. ```faers-toolkit/data/faers_ascii_2018q1.zip```). Next run the parser:

```python
python3 parse.py
```

The outputted sqlite database will be saved in ```faers-toolkit/faers-data-sqlite.zip```. To use the database for the other tools, extract the database into the ```faers-toolkit/db/``` folder.


### Removing duplicate entries

We can remove duplicate entries from our database by running the following script:

```python
python3 dedeplicate.py
```

The FAERS dataset contains cases which have received multiple entries. By the FDA's recommendation, this script removes all old versions of each case. For cases existing in both FAERS and AERS, the AERS cases are eliminated by default. In FAERS, this will consider the "caseid" and "caseversion" attributes and keep only the case with the highest (most recent) version number. In AERS, this will consider the version with the highest "ISR" as the most recent.

Note: This does not account for all duplicate entries in the database. The existence of these entries is an inherent flaw in FAERS which should be taken into consideration when using FAERS data for statistical analysis.