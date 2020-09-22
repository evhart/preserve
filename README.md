# 🥫 Preserve - A simple Python Key/Value database with multiple backends.

> ⚠️ Preserve is alpha software and currently in development (i.e., no tests).

Preserve is a simple (simplistic) key/value store for storing JSON-like data in different backends. Its API is based on the standard Python dictionary API.


## ℹ️ Installation and Usage
Preserve can be installed using pip:
```
pip install preserve
```

Preserve can be also installed from Github directly using the following command:
```
pip install git+https://github.com/evhart/preserve#egg=preserve
```

### 📒 Requirements
Preserve needs the following libraries installed and Python 3.6+ (tested on Python 3.8):
* [halo](https://github.com/manrajgrover/halo)
* [pydantic](https://pydantic-docs.helpmanual.io/)
* [pymongo](https://pymongo.readthedocs.io/)
* [tabulate](https://github.com/astanin/python-tabulate)
* [typer](https://typer.tiangolo.com/)

### 🐍 Python API

If you know how to use Python dictionaries, you already know how to use preserve. Simply use the backend connector that corresponds to your database and you are ready to go.

You can either create a new database from a standarised database URI or using the driver parameters:

```python
import preserve

# Using parameters:
jam_db1 = preserve.open('shelf', filename="preserve.dbm")
jam_db1['strawberry'] = {'name': 'Strawbery Jam', 'ingredients': ['strawberry', 'sugar']}


# Using URI:
jam_db2 = preserve.from_uri("mongodb://127.0.0.1:27017/preserves?collection=jam")
jam_db2['currant'] = {'name': 'Currant Jam', 'ingredients': ['currant', 'sugar']}

```

### 🖥️ Command Line Interface (CLI)
Preserve has a simple CLI utility that can be access using the ```preserve``` command. Preserve support migrating/exporting data from one database ot another database and showing the firs rows from databases.

```
Usage: preserve [OPTIONS] COMMAND [ARGS]...

  🥫 Preserve - A simple Key/Value database with multiple backends.

Options:
  --install-completion [bash|zsh|fish|powershell|pwsh]
                                  Install completion for the specified shell.
  --show-completion [bash|zsh|fish|powershell|pwsh]
                                  Show completion for the specified shell, to
                                  copy it or customize the installation.

  --help                          Show this message and exit.

Commands:
  connectors  List available connectors.
  export      Export a database to a different output.
  header      Get header of a given database table.
```
