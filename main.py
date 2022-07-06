
import pandas as pd
import os
import glob
import utils
import pyodbc
import logging

from config import *

logging.basicConfig(filename='importer.log',format='%(asctime)s - %(message)s')

if __name__ == "__main__":

    query_path = os.path.dirname(os.path.abspath(__file__))
    query_path = os.path.join(query_path,"imports_sql")
    
# create list of files to be imported (files group in subfolder are joined)
    files_list= []
    if os.path.isdir(path):
        for f in glob.glob(path + "\*.csv"):
            files_list.append(f)
        subdirs =  [ os.path.join(path,sd) for sd in os.listdir(path) if os.path.isdir(os.path.join(path,sd))]
        for sd in subdirs:
            if utils.join_csv(sd) == "error":
                logging.error(f" {sd} contains csvs with diff cols, folder skipped")
                continue
            else:
                files_list.append(utils.join_csv(sd))
    elif os.path.isfile(path) and path.split(".")[-1:][0] =="csv":
        files_list.append(path)

    if len(files_list) == 0:
        print("path is incorrect - no csv found")


# create temp files and list of dict containing: key = temp file path, values = list of dicts with column name and type 
    files_cols =[]
    for f in files_list:

        content = pd.read_csv(f,sep = separator)
        print(f)
        content.fillna("",inplace=True)
        sql_columns_types =[]

        for col in content.columns:
            if content.dtypes.loc[col] =="int64" or content.dtypes.loc[col] =="float64":
                sql_columns_types.append(utils.define_sql_type(col,content[col],type="nums"))
            elif content.dtypes.loc[col] =="object":
                try:
                    content[col] = pd.to_numeric(content[col])
                    num_type = utils.define_sql_type(col,content[col],type="nums")
                    if list(num_type.values())[0] == "int":
                        content[col] = content[col].astype("Int64")
                    sql_columns_types.append(num_type)

                except:
                    try:
                        content[col] = pd.to_datetime(content[col])
                        sql_columns_types.append({col: "datetime"})
                    except:
                        sql_columns_types.append(utils.define_sql_type(col,content[col]))


        if "tempfileforimporter" in f:
            content.to_csv(f,index = False,sep=separator)
        else:
            files_list.remove(f)
            f = os.path.dirname(f) + "\\" + os.path.basename(f).replace(".csv","_tempfileforimporter.csv")
            files_list.insert(0,f)
            content.to_csv(f,index = False,sep=separator)
        f_c = {f:sql_columns_types}
        files_cols.append(f_c)

# execute sqls generated by gen_sql_file
    try:
        if trusted_connection == 1:
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes;')
        else:
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

        cursor = conn.cursor()

        for f_c in files_cols:
            q = utils.gen_sql_file(f_c)
            cursor.execute(q)
            cursor.commit()
    except Exception as e:
        print(str(e))
        logging.error(str(e))
    finally:
        for f in files_list:
            os.remove(f)
        conn.close()





