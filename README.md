# PyCoronaData

Python wrapper for [Johns Hopkins data](https://github.com/CSSEGISandData/COVID-19).

## Dependancies
- Python 3.7>
- Pandas
- GeoPandas

## Features
- Fetch and construct a pandas dataframe based on Johns Hopkins data
- Estimate the number of recovery cases for each day
- Add countries informations (geometry, population size) for each country and clean country information from raw data
- Add some statistics such as letaly rate, number of cases per 100.000 habitants for each country
- Afford a persistent mode for interactive application, allowing dynamic update

## Install
Using pip
```bash
pip install git+https://github.com/jsgounot/pycoronadata.git
```
Or download / clone the github
```bash
git clone https://github.com/jsgounot/pycoronadata.git
cd pycoronadata
python setup.py install --user
```

## Quick manual
Produce a simple dataframe from John Hopkins raw data with longitude and latitude as pivot point
```python3
from pycoronadata import CoronaData
cd = CoronaData(["Lat", "Long"])
print (cd.cdf)
```
Combine both raw data and geographical data
```python3
from pycoronadata import GeoCoronaData

# Default pivot is Country
cd = GeoCoronaData()

# Extract data from report 58 with values for missing countries
cd.data_from_day(58, report=True, fill=True)

# Same but with continents instead of country
cd.data_from_day(58, report=True, fill=True, geocolumn="Continent"))

# Grab data from Africa with all the time period
cd.data_from_geocol(select="Africa", geocolumn="Continent", fill=False)
```
Persistant mode : Load and save data into a file 
```python3
from pycoronadata import PersistantGeoCoronaData

cd = PersistantGeoCoronaData(file_path)
cd.update()
cd.save()
```

## About data columns
**CoronaData and following**
| ID        	| Description                                              	|
|-----------	|----------------------------------------------------------	|
| RepDays   	| Days passed since first report (2020-03-02)              	|
| Recovered 	| Number of recovered (see below for how it is calculated) 	|
| Active    	| Number of active cases                                   	|
| CODay     	| New confirmed cases of the day                           	|
| REDay     	| New recovered cases of the day                           	|
| DEDay     	| New deaths cases of the day                              	|
| LRate     	| Letality rate                                            	|

**GeoCoronaData and persistant mode**

| ID        	| Description                                                                	|
|-----------	|----------------------------------------------------------------------------	|
| Country   	| Country related to each entry, confirmed using longitude and latitude data 	|
| ADMO_3    	| Country code                                                               	|
| SubRegion 	| Entry sub region                                                           	|
| REGION_WB 	| Entry world regions (i.e South Asia)                                       	|
| Continent 	| Entry continent                                                            	|
| PopSize   	| Population size (2018) for either a country or a region                    	|
| PrcCont   	| Percent of the population                                                  	|
| CO10K     	| Number of confirmed per 100,000 habitants                                  	|
| DE10K     	| Number of deaths per 100,000 habitants                                     	|
| RE10K     	| Number of recovered per 100,000 habitants                                  	|
| AC10K     	| Number of actives per 100,000 habitants                                    	|

## How is calculated the number of recovery ?
Since [this report](https://github.com/CSSEGISandData/COVID-19/issues/1250), recovered cases are no longer provided. To get an estimation of recovered cases for each day / country, one can define a mean value of the disease period until recovery which by default is set to 14 days. With this, the number of recovered cases is then linked to both confirmed cases from X previous day and the number of deaths at a given time. Note that the value provided here is therefore only an estimation and does not reflect reality. To change the communicability period, modify the `rtime` parameter during instance construction.

## See also
**[CoronaTools](https://github.com/jsgounot/CoronaTools)** : Dashboard of corona data using bokeh