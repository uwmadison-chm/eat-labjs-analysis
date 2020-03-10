import sys
import csv
import json

# TODO: Never implemented because we weren't able to embed existing Empathic 
# Accuracy task in qualtrics due to IRB permissions requiring the videos be 
# behind a login/password

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

