import sys
import csv
import json

with open(sys.argv[1], newline='', encoding='utf16') as tsvfile:
    reader = csv.DictReader(tsvfile, dialect=csv.excel_tab)
    # First two rows out of Qualtrics are header-y stuff
    next(reader)
    next(reader)
    for row in reader:
        stuff = row['labjs-data']
        if not stuff:
            stuff = "{}"
        o = json.loads(stuff)
        print(o[0])
        print(len(o))

