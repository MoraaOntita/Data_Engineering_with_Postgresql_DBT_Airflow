# Makefile

# Define variables
PYTHON = python
DB_CONNECTION_SCRIPT = scripts/data_models_scripts/db_connections.py
CREATE_TABLES_SCRIPT = scripts/data_models_scripts/create_tables.py
MAIN_SCRIPT = scripts/data_models_scripts/main.py

# Define targets
.PHONY: run create_tables main

# Default target
run: create_tables main

# Target to create tables
create_tables:
	$(PYTHON) $(DB_CONNECTION_SCRIPT)
	$(PYTHON) $(CREATE_TABLES_SCRIPT)

# Target to run the main script
main:
	$(PYTHON) $(MAIN_SCRIPT)



