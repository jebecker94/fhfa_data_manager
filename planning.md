# Project Planning

- [ ] Add request timeouts and a reusable `requests.Session` with user-agent headers in `_download_from_page` to avoid hanging downloads and improve resiliency. 
- [ ] Replace print statements in `FHFADataLoader` workflows with the standard `logging` module so callers can manage verbosity.
- [ ] Move the unused `pyodbc` import (and dependency) behind an optional extra to simplify installation for users who do not need Access/ODBC features.
- [ ] Introduce lightweight unit tests around dictionary resolution helpers (`resolve_dictionary_for_data_file` / `resolve_dictionaries_for_year_folder`) to lock in expected mappings.
- [ ] Refactor `config.py` to reuse `PathsConfig` instead of duplicating environment variable handling.
