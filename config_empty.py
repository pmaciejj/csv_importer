import os
import shutil

# path to csv or folder
path = r""

separator = ""

### db connection 
server=""
database=""
trusted_connection = False
username = ""
password = ""

### bulk / row
insert_mode = ""

query_path = None

if query_path is None:
    query_path = os.path.dirname(os.path.abspath(__file__))
    query_path = os.path.join(query_path,"imports_sql")
if  os.path.isdir(query_path):
    shutil.rmtree(query_path)
os.mkdir(query_path)


