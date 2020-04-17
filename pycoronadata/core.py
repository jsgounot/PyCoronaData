# -*- coding: utf-8 -*-
# @Author: jsgounot
# @Date:   2020-03-14 23:50:19
# @Last modified by:   jsgounot
# @Last Modified time: 2020-04-14 15:33:55

import os
rpath = os.path.realpath(__file__)
bname = os.path.basename
dname = os.path.dirname

from datetime import datetime
from functools import lru_cache

import logging
import zipfile
import json

import numpy as np
import pandas as pd

import geopandas as gpd

from shapely.geometry import Point
from shapely.ops import cascaded_union

from pycoronadata import utils 

TESTING = False

TIME_SERIES = [
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
    ]

COUNTRY_REGiONS = {"Continent", "SubRegion", "REGION_WB", "ADM0_A3"}

class CoronaData() :

    ALLOWED_GB = {"Province/State", "Country/Region", "Lat", "Long"}

    def __init__(self, gb, rtime=14, logger=None, head=0) :
        """        
        Object which fetch and contains coronadata from the Johns Hopkins Institut
        https://github.com/CSSEGISandData/COVID-19
        
        Arguments:
            gb {[string or string list]} -- [Columns to aggregate]
        
        Keyword Arguments:
            rtime {int} -- [Recovery time] (default: {14})
            logger {[logging.Logger]} -- [Logger] (default: {None})
            head {int} -- [Number of row if a subset is needed (dataframe.head)] (default: {0})
        """

        self.logger = logger or logging.getLogger(utils.LOG_NAME)
        logger.debug("Initiate CoronaData instance ...")

        self.check_inputs(gb)
        self._gb = gb

        logger.debug("Load cdf file")
        self._cdf = self.load_cdf(rtime, head)
        logger.debug("Finish instance")

    @property
    def gb(self):
        return self._gb
    
    @property
    def cdf(self):
        return self._cdf

    def allowed_gb(self) :
        return CoronaData.ALLOWED_GB

    def check_inputs(self, gb) :
        # Check gb :
        gb = [gb] if isinstance(gb, str) else gb       
        supplementals = set(gb) - self.allowed_gb()
        if supplementals : 
            raise ValueError(f"Unknown gb : {supplementals} - Allowed : {CoronaData.ALLOWED_GB}")

    def load_cdf(self, rtime, head=0) :
        cdf = self.generate_cdf()
        cdf = self.setup_cdf(cdf, rtime)
        if head : cdf = cdf.head(head)
        return cdf

    def setup_cdf(self, cdf, rtime) :
        cdf = self.add_recovery_time_cdf(cdf, rtime)
        cdf = self.add_daily_cases_cdf(cdf)
        cdf = self.add_stats_cdf(cdf)
        return cdf

    def set_recovery_time(self, rtime) :
        """
        Modify the recovery time to use
        
        Arguments:
            rtime {[int]} -- [Recovery time]
        """

        self.logger.info(f"Change recovery time to : {rtime}. Can take time ...")
        self._cdf = self.setup_cdf(self.cdf, rtime=rtime)

    def unique(self, column) :
        return sorted(self.cdf[column].unique())

    def days(self, report=False) :
        column = "RepDays" if report else "Date"
        return self.unique(column)

    def firstday(self, report=False) :
        column = "RepDays" if report else "Date"
        return self.cdf[column].min()

    def lastday(self, report=False) :
        column = "RepDays" if report else "Date"
        return self.cdf[column].max()

    # ----------------------------------------------------------------------------------------------------------------
    # Data generation

    @staticmethod
    def repDays(days) :
        days = pd.to_datetime(days).dt.date
        return (days - days.min() + pd.Timedelta('1 days')).dt.days

    @staticmethod
    def load_from_time_serie(url, logger=None) :
        if logger : logger.info(f"Fetch from : {url}")
        name = bname(url).split("_")[3].title()

        df = pd.read_csv(url, sep=",")
        df = pd.melt(df, id_vars=df.columns[:4], value_vars=df.columns[4:], var_name="date", value_name=name)
        df.columns = [column.title() for column in df.columns]

        return df

    @staticmethod
    def corona_data_from_time_series(logger=None) :
        data = [GeoCoronaData.load_from_time_serie(url, logger) for url in TIME_SERIES]
        df = data.pop(0)

        while data :
            df = df.merge(data.pop(0), on=list(df.columns[:5]))

        return df

    @staticmethod
    def manual_correction(cdf) :
        cdf = cdf[cdf["Province/State"] != "Recovered"]
        return cdf

    @staticmethod
    def find_country_lon_lat(gdf, lon, lat, guess=None) :
        """
        Find country name based on longitude and latitude values
        
        Arguments:
            gdf {dict} -- [{country : polygone} dictionary]
            lon {[float]} -- [Longitude]
            lat {[float]} -- [Latitude]
        
        Keyword Arguments:
            guess {[str]} -- [The country where the location is most likely found] (default: {None})
        
        Returns:
            [str] -- [Country where the location is found, else np.nan]
        """

        point = Point(lon, lat)

        if guess in gdf and gdf[guess].contains(point) :
            return guess

        for country, polygone in gdf.items() :
            if polygone.contains(point) :
                return country
        return np.nan

    def add_recovery_time_cdf(self, cdf, rtime) :
        cdf["Date"] = pd.to_datetime(cdf["Date"], infer_datetime_format=True).dt.date

        subdf = cdf.copy()
        subdf["RepDays"] = subdf["RepDays"] + rtime

        columns = self.gb + ["RepDays", "Confirmed"]
        subdf = subdf[columns]
        
        columns = self.gb + ["RepDays", "Recovered"]
        subdf.columns = columns

        columns = self.gb + ["RepDays"]
        cdf = cdf.merge(subdf, on=columns, how="left")

        cdf["Recovered"] = cdf["Recovered"] - cdf["Deaths"]
        cdf["Recovered"] = cdf["Recovered"].fillna(0).astype(int)
        
        # Maybe this line should be added
        # Sometimes you can have more deaths than the number of case (rtime) before
        # Especially at the start of the infection in one country
        # This variation also affect the REDay. Patch after does not correct REDay
        # cdf.loc[cdf["Recovered"] < 0, "Recovered"] = 0

        cdf["Active"] = cdf["Confirmed"] - (cdf["Deaths"] + cdf["Recovered"])

        return cdf

    def add_daily_cases_cdf(self, cdf) :
        subdf = cdf.copy()
        subdf["RepDays"] = subdf["RepDays"] + 1

        columns = ["Confirmed", "Recovered", "Deaths"]
        columns_old = [column + "Old" for column in columns]

        mcolumns = self.gb + ["RepDays"]
        subdf = subdf[mcolumns + columns]

        cdf = cdf.merge(subdf, on=mcolumns, suffixes=("", "Old"), how="left")
        cdf[columns_old] = cdf[columns_old].fillna(0).astype(int)

        for idx, column in enumerate(columns_old) :
            nname = column[:2].upper() + "Day"
            cdf[nname] = cdf[columns[idx]] - cdf[column]

        return cdf.drop(columns_old, axis=1)

    def add_stats_cdf(self, cdf) :
        # Letality rates
        cdf["LRate"] = cdf["Deaths"] / cdf[["Deaths", "Recovered"]].sum(axis=1)
        cdf["LRate"] = cdf["LRate"].fillna(0)  

        return cdf

    def generate_cdf(self) :
        cdf = GeoCoronaData.corona_data_from_time_series(self.logger)

        # Manual correction
        cdf = GeoCoronaData.manual_correction(cdf)

        # We remove row for which nothing is found
        cdf = cdf[cdf[["Confirmed", "Deaths"]].sum(axis=1) != 0]
        
        # We groupby geocols and date
        columns = self.gb + ["Date"]
        cdf = cdf.groupby(columns)["Confirmed", "Deaths"].sum().astype(int).reset_index()
        cdf["RepDays"] = GeoCoronaData.repDays(cdf["Date"])

        columns = self.gb + ["RepDays"]
        cdf = cdf.sort_values(columns)

        return cdf

class GeoCoronaData(CoronaData) :

    GEOCOLS = {"Country", "Continent", "SubRegion", "REGION_WB", "REGION_UN", "ADM0_A3"}

    def __init__(self, geofile=None, rtime=14, logger=None, head=0) :
        """        
        Object containing both corona data and geographic information
        All data are from a geo
        
        Keyword Arguments:
            geofile {[str]} -- [geofile containing countries information (geojson or shapefile)] (default: {None})
            rtime {int} -- [average infection time allowing to estimate the number of recovered cases] (default: {14})
            logger {[logging.Logger]} -- [logger] (default: {None})
            head {int} -- [Number of row if a subset is needed (dataframe.head)] (default: {0})
        """

        logger = logger or logging.getLogger(utils.LOG_NAME)
        logger.debug("Create GeoCoronaData instance")
        
        logger.debug("Load GDF")
        self._gdf = self.load_gdf(geofile)
        
        logger.debug("Initiate primary CoronaData instance")
        super().__init__(rtime=rtime, logger=logger, head=head, gb=["Country"])

    def allowed_gb(self) :
        return set(["Country"])

    def load_gdf(self, geofile=None, default_detail=10) :
        if geofile : return self.load_custom_gdf(geofile)
        else : return self.load_internal_gdf(default_detail)
    
    def load_custom_gdf(self, geofile) :
        if geofile.endswith(".zip") : geofile = "zip:///" + geofile
        return gpd.read_file(geofile)

    def load_internal_gdf(self, default_detail) :
        columns = ['ADMIN', 'geometry', "ADM0_A3", "POP_EST", "CONTINENT", "REGION_UN", "SUBREGION", "REGION_WB"]
        renamed = {'ADMIN' :  'Country', 'POP_EST' : 'PopSize', 'CONTINENT' : 'Continent', 'SUBREGION' : 'SubRegion'}

        fname = self.default_geofile(default_detail)
        df = gpd.read_file(fname)[columns]
        df.columns = [renamed.get(column, column) for column in columns]
        return df

    def default_geofile(self, detail=10):
        return os.path.join(dname(rpath), "geodata", f"ne_{detail}m",
            f"ne_{detail}m_admin_0_countries.shp")

    @property
    def gdf(self):
        return self._gdf
    
    def add_geom_light(self, cdf, column="Country") :
        return self.add_geom(cdf, column, default_detail=110)

    def add_geom(self, cdf, column="Country", geofile=None, default_detail=None) :
        if column not in GeoCoronaData.GEOCOLS :
            raise ValueError(f"Column '{geocol}' is not a allowed geo column : {GeoCoronaData.GEOCOLS}")

        mapper = self.make_geo_mapper(column, geofile, default_detail)
        cdf["geometry"] = cdf[column].map(mapper)
        return cdf

    @lru_cache(maxsize=10)
    def make_geo_mapper(self, column, geofile=None, default_detail=None) :
        if geofile or default_detail : gdf = self.load_gdf(geofile=geofile, default_detail=default_detail) 
        else : gdf = self.gdf

        # Merge polygones if needed, i.e Continents
        if column in ("Country", "ADM0_A3") :
            mapper = gdf.set_index(column)["geometry"]
        
        else :
            # https://stackoverflow.com/questions/31391209/valueerror-no-shapely-geometry-can-be-created-from-null-value
            gdf["geometry"] = [geom if geom.is_valid else geom.buffer(0) for geom in gdf["geometry"]]
            mapper = gdf.groupby(column)["geometry"].apply(cascaded_union)

        return mapper

    def df2gdf(self, cdf, * args, light=False, ** kwargs) :
        fun = self.add_geom_light if light else self.add_geom
        cdf = fun(cdf, * args, ** kwargs)
        return gpd.GeoDataFrame(cdf)

    def save_geojson(self, fname, cdf, column) :
        if not "geometry" in cdf.columns : 
            raise ValueError("geometry not found in dataframe, use add_geom function before")
    
        gdf = self.df2gdf(cdf, column)
        self.logger.info(f"Save cdf to geojson at : {fname}")
        gdf.to_file(fname, driver='GeoJSON') 

    def order_cdf(self, cdf) :
        order = ["Country", "ADM0_A3", "SubRegion", "REGION_WB", "Continent", "PopSize", "Date", "RepDays", "Confirmed", "Deaths",
                "Recovered", "Active", "CODay", "REDay", "DEDay", "LRate", "PrcCont", "CO10K", "DE10K", "RE10K", "AC10K"]

        funsort = lambda name : order.index(name)
        columns = sorted(cdf.columns, key=funsort)
        return cdf[columns]

    # ----------------------------------------------------------------------------------------------------------------
    # Data generation

    def setup_cdf(self, cdf, rtime) :
        cdf = super().setup_cdf(cdf, rtime)
        cdf = self.add_PopInfo_cdf(cdf)
        return self.order_cdf(cdf)

    def add_PopInfo_cdf(self, cdf) :
        columns = ["Confirmed", "Deaths", "Recovered", "Active"]
        cdf["PrcCont"] = cdf[columns[:3]].sum(axis=1) / cdf["PopSize"]

        for column in columns :
            nname = column[:2].upper() + "10K"
            cdf[nname] = cdf[column] * 10000 / cdf["PopSize"]

        return cdf

    def generate_cdf(self) :
        cdf = GeoCoronaData.corona_data_from_time_series(self.logger)

        # Manual correction
        cdf = GeoCoronaData.manual_correction(cdf)

        # We remove row for which nothing is found
        cdf = cdf[cdf[["Confirmed", "Deaths"]].sum(axis=1) != 0]

        # We confirm country using longitude and latitue
        # since gdf countries does not have the same name than cdf data
        gdfd = self.gdf.set_index("Country")["geometry"].to_dict()
        unique_coor = set(zip(cdf["Long"], cdf["Lat"], cdf["Country/Region"]))
        unique_coor = {coor[:2] : GeoCoronaData.find_country_lon_lat(gdfd, * coor)
            for coor in unique_coor}

        # We add country if country is found inside gdf
        cnames = set(self.gdf["Country"])
        cdf["GCountry"] = cdf["Country/Region"].apply(lambda x : x if x in cnames else np.nan)

        # We map results and remove previous country column from cdf name
        fun_mapping = lambda row : unique_coor[(row["Long"], row["Lat"])]
        cdf.loc[cdf["GCountry"].isna(), "GCountry"] = cdf[cdf["GCountry"].isna()].apply(fun_mapping, axis=1)

        # Missing countries we were not able to found
        missing = cdf[cdf["GCountry"].isna()]["Country/Region"].unique()
        self.logger.warning(f"Countries not found with geodata (will be ignored) : {missing}")

        # We group by country and date
        columns = ["GCountry", "Date"]
        cdf = cdf.groupby(columns)["Confirmed", "Deaths"].sum().astype(int).reset_index()
        cdf.columns = [{"GCountry" : "Country"}.get(column, column) for column in cdf.columns]

        # Add country info used by groupby
        gdf = self.gdf[["Country", "PopSize", "Continent", "SubRegion", "REGION_WB", "ADM0_A3"]]
        cdf = cdf.merge(gdf, on="Country", how="left")

        cdf["RepDays"] = GeoCoronaData.repDays(cdf["Date"])
        cdf = cdf.sort_values(["Country", "RepDays"])

        return cdf

    # ----------------------------------------------------------------------------------------------------------------
    # API like side

    def fill_subdf_geo(self, subdf, column, filler={}) :
        if column not in GeoCoronaData.GEOCOLS :
            raise ValueError(f"Column '{column}' is not a allowed geo column : {GeoCoronaData.GEOCOLS}")

        gdf = self.gdf.groupby(column)["PopSize"].sum().reset_index()
        for key, value in filler.items() : gdf[key] = value

        columns = ["Confirmed", "Deaths", "Recovered", "CODay", "REDay", "DEDay", "Active"]
        subdf = subdf.groupby([column, "Date", "RepDays"])[columns].sum().reset_index()

        subdf = gdf.merge(subdf, on=[column, "Date", "RepDays"], how="left")
        subdf[columns] = subdf[columns].fillna(0).astype(int)
        
        subdf = self.add_stats_cdf(subdf)
        subdf = self.add_PopInfo_cdf(subdf)

        return subdf

    def data_from_day(self, day=None, report=False, fill=False, geocolumn="Country", as_datetime=False) :
        """
        DataFrame for a given time
        
        Keyword Arguments:
            day {[str/int]} -- [Date or report day] (default: {None})
            report {bool} -- [Use report day or date] (default: {False})
            fill {bool} -- [Fill with missing geocolumn values] (default: {False})
            geocolumn {str} -- [GeoColumn to pivot with] (default: {"Country"})
            as_datetime {bool} -- [Convert date as datetime instead than datetime.dt] (default: {False})
        
        Returns:
            [DataFrame] -- [DataFrame with given values]

        Raises:
            ValueError -- [Raised when uncorrect geocolumn is provided]
        """

        column = "RepDays" if report else "Date"
        day = day or self.cdf[column].max()
        cdf = self.cdf[self.cdf[column] == day]

        if cdf.empty :
            raise ValueError(f"Nothing found for this date {day}")

        if fill : 
            filler = {"Date" : next(iter(cdf["Date"])), "RepDays" : next(iter(cdf["RepDays"]))}
            cdf = self.fill_subdf_geo(cdf, geocolumn, filler=filler)

        if as_datetime :
            transform_date = lambda date : datetime.combine(date, datetime.min.time())
            cdf["Date"] = cdf["Date"].apply(transform_date)

        return self.order_cdf(cdf)

    def data_from_geocol(self, select, geocolumn, fill=False, as_datetime=False) :
        """
        DataFrame for a given location
        
        Arguments:
            select {[str]} -- [Location value]
            geocolumn {[str]} -- [GeoColumn to use]
        
        Keyword Arguments:
            fill {bool} -- [Fill with missing dates] (default: {False})
            as_datetime {bool} -- [Convert date as datetime instead than datetime.dt] (default: {False})
        
        Returns:
            [DataFrame] -- [DataFrame with given values]
        
        Raises:
            ValueError -- [Raised when uncorrect geocolumn is provided]
        """

        if geocolumn not in GeoCoronaData.GEOCOLS :
            raise ValueError(f"Column '{geocolumn}' is not a allowed geo column : {GeoCoronaData.GEOCOLS}")

        cdf = self.cdf[self.cdf[geocolumn] == select]

        if cdf.empty :
            self.logger.info(f"Select value {select} not found in current cdf. Empty dataframe returned")
            return pd.DataFrame()
        
        if geocolumn not in ("Country", "ADM0_A3") :
            columns = ["Confirmed", "Deaths", "Recovered", "CODay", "REDay", "DEDay", "Active"]
            cdf = cdf.groupby([geocolumn, "Date", "RepDays"])[columns].sum().reset_index()
            
            mapper = self.gdf.groupby(geocolumn)["PopSize"].sum()
            cdf["PopSize"] = cdf[geocolumn].map(mapper)

            cdf = self.add_stats_cdf(cdf)
            cdf = self.add_PopInfo_cdf(cdf)

        else :
            columns = ["Country", "Continent", "SubRegion", "REGION_WB", "ADM0_A3"]
            columns.remove(geocolumn)
            cdf = cdf.drop(columns, axis=1)

        if fill :
            sdf = pd.DataFrame(pd.Series(self.days(), name="Date"))
            sdf["RepDays"] = self.repDays(sdf["Date"])
            sdf[geocolumn] = next(iter(cdf[geocolumn]))
            sdf["PopSize"] = next(iter(cdf["PopSize"]))

            columns = list(set(cdf.columns) - set(sdf.columns))
            for column in columns : sdf[column] = 0

            cdf = pd.concat((cdf, sdf))
            cdf = cdf.drop_duplicates("RepDays").sort_values("RepDays")

        if as_datetime :
            transform_date = lambda date : datetime.combine(date, datetime.min.time())
            cdf["Date"] = cdf["Date"].apply(transform_date)

        return self.order_cdf(cdf)

class PersistantGeoCoronaData(GeoCoronaData) :

    def __init__(self, * args, fname=None, utime=None, rtime=14, ** kwargs) :      
        self._fname = utils.TMPFname(ext="csv") if fname is None else fname
        super().__init__(* args, rtime=rtime, ** kwargs)
        self._rtime = rtime
        self._watcher = utils.WatchFile(self.fname, utime=utime, logger=self.logger)

    @property
    def fname(self):
        return str(self._fname)    

    @property
    def rtime(self):
        return self._rtime
    
    @property
    def watcher(self):
        return self._watcher
    
    @property
    def istemp(self):
        return isinstance(self._fname, utils.TMPFname) 

    def check_inputs(self, * args, ** kwargs) :
        if isinstance(self._fname, utils.TMPFname) : 
            self.logger.info("Path not provided, generate a temporary file")
        
        elif isinstance(self._fname, str) :
            if not os.path.isfile(self._fname) :
                self.logger.info(f"Non existing file ({self._fname}), generate new data from remote")
            else :
                self.logger.debug(f"Link PGCD to file path : {self._fname}")

        else :
            self.logger.error(f"File path must be a string (path) or None, got {type(self._fname)}")
            raise ValueError(f"File path must be a string (path) or None, got {type(self._fname)}")

        super().check_inputs(* args, ** kwargs)

    def load_cdf(self, rtime, head=0) :
        if self.fname and os.path.isfile(self.fname) and not self.istemp :
            df = pd.read_csv(self.fname)
            df["Date"] = pd.to_datetime(df["Date"], infer_datetime_format=True).dt.date
            return df

        else :
            return super().load_cdf(rtime, head)

    def save(self) :
        self.logger.debug(f"Save cdf to file name : {self.fname}")
        self.cdf.to_csv(self.fname, index=False)

    def set_recovery_time(self, rtime) :
        self.rtime = rtime
        super().set_recovery_time(rtime)

    def update_cdf(self) :
        self.logger.info(f"Run cdf update for {self}")
        cdf = self.generate_cdf()
        cdf = self.setup_cdf(cdf, self.rtime)
        self._cdf = cdf
        return cdf

    def update(self) :
        if self.watcher.check_update() :
            self.update_cdf()