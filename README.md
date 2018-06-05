FAERS Toolkit
=============
This repository provides tools for data analysis from the [FDA Adverse Event Reporting System **(FAERS)**](https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/). The toolkit accesses FAERS data which is [publicly available online](https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm), parsed into a SQLite database with **[wizzl35's script](https://github.com/wizzl35/faers-data)**. I am not responsible for any conclusions drawn from the data, nor do I guarantee its efficacy.

### What can FAERS Toolkit do?
* [Remove duplicate entries from data](#removing-duplicate-entries)
* Calculate signal scores (under construction)

## Getting Started

In order to use FAERS Toolkit, we must set up the database.

First, clone the repository. Next, retrieve the output file 'faers-data.sqlite' by using **[wizzl35's script](https://github.com/wizzl35/faers-data)** and place it in the 'faers-toolkit/data/db/' directory. 

In order to see if the database as been imported correctly, run:
```python
python3 test.py
```

You should see the following output with no errors or any additional outputs.
```
Connected to FAERS database.
Disconnected from FAERS database.
``` 

## Using FAERS Toolkit
### Removing duplicate entries

We can remove duplicate entries from our database by running the following script:

```python
python3 dbcleanup.py
```


The FAERS dataset contains cases which have received multiple entries. This is because each unique submission of a case is added into FAERS, including followup reports from the same patient/drugs/adverse events combination. Thus, we are given multiple versions of each case. By the FDA's recommendation, this script removes all old versions of each case. This leaves only the latest version of each case, which represents the most current information.

Note: This does not account for duplicate entries in that the same report was submitted multiple times (e.g., if both a patient and their doctor submitted a report to FAERS). The existence of these entries is an inherent flaw in FAERS which should be taken into consideration when using FAERS data for statistical analysis.