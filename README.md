# Introduction
This is meant to be a very basic command line program to process CSV files from the Gun Violence Archive to match the new and improved Incidents spreadsheet. As such, this program is not super flexible. Hopefully, this will make data entry significantly easier. This is a short script using Pandas and Geopy.

# Instructions
1. Download Python 3.9 or higher.
2. Pip install `pandas` and `geopy`. I recommend using a venv and the requirements.txt.
3. Download `process_gva_csv.py` to a folder on your computer.
4. Navigate to that folder in your choice of terminal: Bash, Terminal, Powershell.
5. Run `python process_gva_csv.py help` for help. Change this command depending on your OS.
6. Once you done that, run `python process_gva_csv.py target.csv out.csv` in order to process a CSV from the Gun Violence Archive!

