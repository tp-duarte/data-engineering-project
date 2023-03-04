@echo off

rem this script creates a database and table to 
rem monitor inovation data that is feeded with a python script



cd D:\opensense\projeto\script

set PGPASSWORD=admin
psql -h localhost -d postgres -U postgres -w -f database_and_tables_creation.sql


python writing_to_table.py