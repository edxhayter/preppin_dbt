### Preppin DBT

This repository will be used to hold all Preppin Data Challenges Completed in dbt Core

### General Principles:

- Seeds will be used to load in csv's for practice using seeds. Although not best practice (seeds should be used for small generally static data for things like mapping tables), I have opted to load the data in for pratice with loading seeds and for ease of building out the tables in snowflake to run the code on.
- Models will be built in folders for the particular challenges generally following the principles of modularization:
  - staging: removing unecessary columns and renaming others to appropriate names and conducting any appropriate simple transformations
  - intermediary: carrying out more complex operations and any combinations of tables required. The output of this should be a generally usable table for wider questions.
  - final tables: any operations that make sense as specific for a given report (in this case the particular output)
