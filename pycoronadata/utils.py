# -*- coding: utf-8 -*-
# @Author: jsgounot
# @Date:   2020-03-24 16:29:46
# @Last modified by:   jsgounot
# @Last Modified time: 2020-04-17 13:43:22

import os
import tempfile
import datetime

import logging
from logging.handlers import RotatingFileHandler

DEFAULT_LEVEL = logging.INFO
LOG_NAME = "pycoronadata"

class TMPFname() :

    def __init__(self, delete=True, ext="", quiet=False, logger=None, ** kwargs) :
        suffix = self.format_ext(ext)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, ** kwargs)
        
        self.fname = tmp.name 
        self.delete = delete
        self.quiet = quiet
        self.logger = logger or logging.getLogger(LOG_NAME)
        self.logger.debug(f"Create temporary file at : {self.fname}")

    def __str__(self) :
        return self.fname

    def format_ext(self, ext) :
        ext = str(ext)
        if not ext : return ext
        if not ext.startswith(".") : ext = "." + ext.strip()
        return ext

    def exist(self) :
        return os.path.isfile(self.fname)

    def remove(self) :
        if self.exist() and self.delete :
            if not self.quiet : self.logger.debug(f"Delete temporary file at : {self.fname}")
            os.remove(self.fname)

    def __del__(self) :
        self.remove()

class WatchFile() :

    """
    Class to manage a file with update management
    This class simply inform if a file has been modified within X time (utime)
    """

    default_utime = datetime.timedelta(minutes=1)

    def __init__ (self, fname, utime=None, logger=None) :
        if not isinstance(fname, str) : 
            raise ValueError("fname must be a str")

        if utime is not None and not isinstance(utime, datetime.timedelta) :
            raise ValueError("utime must be a datetime.timedelta instance")
        
        self.fname = fname
        self.utime = utime or WatchFile.default_utime
        self.logger = logger or logging.getLogger(LOG_NAME)

    def isfile(self) :
        return os.path.isfile(self.fname)

    def time_next_update(self) :
        if not self.isfile() :
            self.logger.warning(f"File not found : {self.fname}") 
            return

        lastmod = datetime.datetime.fromtimestamp(os.path.getmtime(self.fname))
        currtim = datetime.datetime.now()

        diff = currtim - lastmod 
        next_time = self.utime - diff

        # we remove second for formating
        hours, remainder = divmod(next_time.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        return "{:02}H:{:02}M".format(int(hours), int(minutes))

    def check_update(self, notfound=True) :
        if not self.isfile() : 
            self.logger.warning(f"File not found : {self.fname}")
            self.logger.warning(f"Allow update : {notfound}")
            return notfound

        lastmod = datetime.datetime.fromtimestamp(os.path.getmtime(self.fname))
        currtim = datetime.datetime.now()

        diff = currtim - lastmod 
        change = diff > self.utime

        lastmodh = lastmod.strftime('%Y-%m-%d %H:%M:%S')
        currtimh = currtim.strftime('%Y-%m-%d %H:%M:%S')

        self.logger.info("Trying to update")
        self.logger.info(f"Current time : {currtimh}")
        self.logger.info(f"Last time since modification : {lastmodh}")
        self.logger.info(f"Time since last update : {diff}")
        self.logger.info(f"Time delta for update : {self.utime}")
        self.logger.info(f"Allow update : {change}")

        return change

def default_logger(fname=None, logger=None, stream=True, level=DEFAULT_LEVEL) :
    
    logger = logger or logging.getLogger(LOG_NAME)
    logger.setLevel(level)  
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    if fname :
        file_handler = RotatingFileHandler(fname, 'a', 1000000, 1)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if stream :
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger