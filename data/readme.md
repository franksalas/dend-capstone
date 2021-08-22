
# Source Data

## Crime Data

- http://www.houstontx.gov/police/cs/crime-stats-archives.htm

The crime dataset contains HPD Beat Crime Statistics crime data from the Houston police department and is part of the Uniform Crime Report program or UCR.
It complies official data collected by law enforcement agencies
across the United States. UCR criminal offenses are divided into two major groups: part I and
part II.

Part I offenses are considered to be serious and are broken into two categories: violent and
property crimes; they include murder, rape, robbery, aggravated assault, burglary, theft, and
auto theft.
Part II offenses are all crime classifications other than those defined as Part I. some of those
include: forgery, fraud, vandalism, prostitution, disorderly conduct.

The crime data is a monthly breakdown of Part I crimes for which
HPD wrote police reports. The data shows the number of reports for the following crimes:
murder, rape, robbery, aggravated assault, burglary, theft, and auto theft.

for the crime data, historical monthly data from 2009 to 2018 was used as data after 2018 the strucutre was drastically changed and not enough for the 1 millon rows requirement.


### The original columns  were

| column  | Description |
|---|---|
|date| Date of offense, include month/date/year   |
|Hour| Approximate time when an event occurs, value form 0-24  |
|Offense Type |Type I offense   |  |
|Beat| The geographic area of the city broken down for patrol and statistical purpose  |
| Premise |Identify the type of location where crime occurs (apartment complex, parking lot,
etc.) |
|Block Range|The value range of street     |
|Street Name| Name of the street where the offense occurred |
| Type |Street type, rd, Blvd
|Suffix |N, S, E, W   |
|Offenses| Times offense happen within the time frame     |

## Weather Data


Data sources
- crime
- oepn weather

## Folder Structure

```
data/raw
├── crime_data
│   ├── 2009
│   │   └── csv
│   │       ├── aug09.xlsx
│   │       ├── dec09.xlsx
│   │       ├── jul09.xlsx
│   │       ├── nov09.xlsx
│   │       ├── oct09.xlsx
│   │       └── sep09.xlsx
│   ├── 2010
│   │   └── csv
│   │       ├── apr10.xlsx
│   │       ├── aug10.xlsx
│   │       ├── dec10.xlsx
│   │       ├── feb10.xlsx
│   │       ├── jan10.xlsx
│   │       ├── jul10.xlsx
│   │       ├── jun10.xlsx
│   │       ├── mar10.xlsx
│   │       ├── may10.xlsx
│   │       ├── nov10.xlsx
│   │       ├── oct10.xlsx
│   │       └── sep10.xlsx
│   ├── 2011
│   │   └── csv
│   │       ├── apr11.xlsx
│   │       ├── aug11.xlsx
│   │       ├── dec11.xlsx
│   │       ├── feb11.xlsx
│   │       ├── jan11.xlsx
│   │       ├── jul11.xlsx
│   │       ├── jun11.xlsx
│   │       ├── mar11.xlsx
│   │       ├── may11.xlsx
│   │       ├── nov11.xlsx
│   │       ├── oct11.xlsx
│   │       └── sep11.xlsx
│   ├── 2012
│   │   └── csv
│   │       ├── apr12.xlsx
│   │       ├── aug12.xlsx
│   │       ├── dec12.xlsx
│   │       ├── feb12.xlsx
│   │       ├── jan12.xlsx
│   │       ├── jul12.xlsx
│   │       ├── jun12.xlsx
│   │       ├── mar12.xlsx
│   │       ├── may12.xlsx
│   │       ├── nov12.xlsx
│   │       ├── oct12.xlsx
│   │       └── sep12.xlsx
│   ├── 2013
│   │   └── csv
│   │       ├── apr13.xlsx
│   │       ├── aug13.xlsx
│   │       ├── dec13.xlsx
│   │       ├── feb13.xlsx
│   │       ├── jan13.xlsx
│   │       ├── jul13.xlsx
│   │       ├── jun13.xlsx
│   │       ├── mar13.xlsx
│   │       ├── may13.xlsx
│   │       ├── nov13.xlsx
│   │       ├── oct13.xlsx
│   │       └── sep13.xlsx
│   ├── 2014
│   │   └── csv
│   │       ├── apr14.xlsx
│   │       ├── aug14.xlsx
│   │       ├── dec14.xlsx
│   │       ├── feb14.xlsx
│   │       ├── jan14.xlsx
│   │       ├── jul14.xlsx
│   │       ├── jun14.xlsx
│   │       ├── mar14.xlsx
│   │       ├── may14.xlsx
│   │       ├── nov14.xlsx
│   │       ├── oct14.xlsx
│   │       └── sep14.xlsx
│   ├── 2015
│   │   └── csv
│   │       ├── apr15.xlsx
│   │       ├── aug15.xlsx
│   │       ├── dec15.xlsx
│   │       ├── feb15.xlsx
│   │       ├── jan15.xlsx
│   │       ├── jul15.xlsx
│   │       ├── jun15.xlsx
│   │       ├── mar15.xlsx
│   │       ├── may15.xlsx
│   │       ├── nov15.xlsx
│   │       ├── oct15.xlsx
│   │       └── sep15.xlsx
│   ├── 2016
│   │   └── csv
│   │       ├── apr16.xlsx
│   │       ├── aug16.xlsx
│   │       ├── dec16.xlsx
│   │       ├── feb16.xlsx
│   │       ├── jan16.xlsx
│   │       ├── jul16.xlsx
│   │       ├── jun16.xlsx
│   │       ├── mar16.xlsx
│   │       ├── may16.xlsx
│   │       ├── nov16.xlsx
│   │       ├── oct16.xlsx
│   │       └── sep16.xlsx
│   ├── 2017
│   │   └── csv
│   │       ├── apr17.xlsx
│   │       ├── aug17.xlsx
│   │       ├── dec17.xlsx
│   │       ├── feb17.xlsx
│   │       ├── jan17.xlsx
│   │       ├── jul17.xlsx
│   │       ├── jun17.xlsx
│   │       ├── mar17.xlsx
│   │       ├── may17.xlsx
│   │       ├── nov17.xlsx
│   │       ├── oc17.xlsx
│   │       └── sep17.xlsx
│   ├── 2018
│   │   └── csv
│   │       ├── apr18.xlsx
│   │       ├── feb18.xlsx
│   │       ├── jan18.xlsx
│   │       ├── mar 18.xlsx
│   │       └── may18.xlsx
│   └── premise_codes.csv
└── weather_data
    ├── b5af47a41a784be4c6fca0b53302f0a1.csv
    └── b5af47a41a784be4c6fca0b53302f0a1.json
```