# -*- coding: utf-8 -*-
# @Author: jsgounot
# @Date:   2020-03-25 19:21:15
# @Last modified by:   jsgounot
# @Last Modified time: 2020-03-25 23:07:21

from setuptools import setup, find_packages
import pycoronadata

setup(
    name = "pycoronadata",
    packages = find_packages(),
    version = pycoronadata.__version__,
    author = "jsgounot",
    url = 'https://github.com/jsgounot/PyCoronaData',
    include_package_data = True
)