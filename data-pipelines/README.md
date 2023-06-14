# Repo with M-RIC data pipelines (for Databricks)
# Main pipeline

**File:** `main_pipeline.py`

**Testing location:** `dbw-processing-mric-dev/david.salac/david_data_pipeline`

The purpose of this pipeline is to remove all identifiable
information from raw storage (aka bronze) and store it in
a silver layer. It drops all free text and identifiable
columns defined by the catalogue (available in a separate repo).
It also computes a salted hash of the ClientID column for future
linkages.

In order to use this script, environmental variables need to be defined
(they are listed at the beginning of the script).

**What it does?**
1. Hash columns with client IDs (using salt).
2. Remove all identifiable columns.
3. Rounds all datetime columns to hours (to make identification harder).
4. Round all Day of birth columns to whole months.
5. Write data into Silver storage (including untreated free-text cols).
6. Drop free-text columns and write into Gold storage

# Related files
Data Catalogue: [catalogue.json](catalogue.json)
