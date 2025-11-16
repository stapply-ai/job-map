source .venv/bin/activate
python ashby/main.py --force && python ashby/export_to_csv.py 
python greenhouse/main.py --force && python greenhouse/export_to_csv.py
python lever/main.py --force && python lever/export_to_csv.py
python workable/main.py --force && python workable/export_to_csv.py