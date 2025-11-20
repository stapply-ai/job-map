source .venv/bin/activate
python ashby/main.py && python ashby/export_to_csv.py 
python greenhouse/main.py && python greenhouse/export_to_csv.py
python lever/main.py && python lever/export_to_csv.py
python workable/main.py && python workable/export_to_csv.py