FAERS Toolkit
=============
This repository provides tools for data analysis from the [FDA Adverse Event Reporting System **(FAERS)**](https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/). The toolkit uses FAERS data which is [publicly available online](https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm). It is able to parse FAERS (and older AERS) data into a MySQL or sqlite database and analyze it using a parsing module adapted from [wizzl35's script](https://github.com/wizzl35/faers-data). I am not responsible for any conclusions drawn from the data, nor do I guarantee its efficacy.

This project was created in conjunction with the [Department of Regulatory and Quality Sciences](https://regulatory.usc.edu/) at the USC School of Pharmacy. An academic poster titled ["Data Mining for Drug Safety"](https://drive.google.com/file/d/1kw19tjEb7IVhAwcByoNAzdOuirF-CZhA/view?usp=sharing) was presented at the 2019 USC Symposium.

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

### Calculating Signal Score Reports

1. Specify the drugs to be searched in a .csv file. Write the drug name at the start of each row, and its possible aliases seperated by commas thereafter. An example can be seen in ```faers-toolkit/input/immuno-drugs.csv```

2. Specify the indications to be searched in ```faers-toolkit/input/immuno-indis.csv```. If you do not wish to categorize by indications, ignore this step.

3. In ```faers.py``` edit lines 13 and 14 to include your input files. If you are not categorizing by indications, leave the parameter empty.

When all is set, run the following script:

```python
python3 faers.py
```

Depending on the number of parameters, and your computer speed, this script could take anywhere between a couple minutes to a couple of hours to complete.

A drug report will list every given drugs interaction with every adverse event it was reported with, categorized by indication (if specified). It will include frequency stats (e.g. number of reports) as well as basic signal scores such as PRR and ROR. The reports also include the 95% confidence interval of the ROR.

A sample report can be found in the ```/output/``` folder which uses the sample inputs, ```immuno-drugs.csv``` and ```immuno-indis.csv```

By default, the report is saved as a ```.xlsx``` file, but all the information is stored as a dictionary in the ```info``` variable on Line 16 of ```faers.py```. The API for the info dictionary is as follows:

* info
    * [drug] drug (dict)
      * ['all'] all indications (dict)
        * ['pids'] primaryids/isrs (list)
        * ['aes'] adverse events (counter)
        * ['stats'] stats (dict)
            * [ae] each AE (dict)
            * ['PRR']
            * ['ROR']
  * [indi] each indication (dict)
    * ['pids'] primaryids/isrs (list)
    * ['aes'] adverse events (counter)
    * ['stats'] stats (dict)
      * [ae] each AE (dict)
        * ['PRR']
        * ['ROR']
