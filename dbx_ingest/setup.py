from setuptools import setup, find_packages

import src

setup(
  name = "dbx_ingest",
  version = src.__version__,
  author = src.__author__,
  url = "https://www.erstedigital.com/en/home",
  author_email = "vukola.milenkovic@erstegroup.com",
  description = "Set of functionalities for ingesting data from various sources into Databricks",
  packages = find_packages(include = ["src"]),
  #entry_points={"group_1": "run=src.__main__:main"},
  install_requires = ["setuptools"]
)
