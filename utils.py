import pandas as pd
import os
import glob
import shutil


from config import separator
from config import insert_mode
from config import query_path



class SQLTemplates:
    check_if_table_exists = """SET NOCOUNT ON 
set xact_abort on
begin tran
if not exists (select 1 from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'tablenameplaceholder')"""
    create_tabel = "create table tablenameplaceholder"
    bulk_insert = """BULK INSERT tablenameplaceholder
FROM   'filepathplaceholder'
    WITH
    (
        FIELDTERMINATOR = 'separatorplaceholder',
        ROWTERMINATOR = '\\n', 
        firstrow = 2 
    )"""
    insert_row = "insert into tablenameplaceholder values ( "


def define_sql_type(col_name,s,type ="strings"):
    # defines sql type int/decimal/nvarchar()
    if type == "strings":
        df = s.to_frame()
        df["len"] = df[col_name].str.len()
        return {col_name: "nvarchar(" + str(df["len"].max()).replace(".0","") + ")"}
    elif type == "nums":
        df = s.to_frame()
        df.dropna(inplace=True)
        df["num_int"] = df[col_name] % 1
        if df["num_int"].sum() == 0:
            return  {col_name: "int"}
        else:
            split = df[col_name].astype("string").str.split(".",expand = True)
            l = split[0].str.len().max()
            r = split[1].str.len().max()
            return  {col_name: "decimal(" + str(l+r) + "," + str(r) + ")"}
    else:
        "error"

def join_csv(path):
    # if one csv in path, creates copy with new name, if multiple performs union (if columns are the same) and creates new file
    files = glob.glob(path + "\*.csv")
    if len(files) == 1:
        f_split = files[0].split("\\")
        new_file = "\\".join(f_split[0:-1]) + "\\"+f_split[-2] + "_tempfileforimporter.csv"

        shutil.copy2(files[0],new_file)

        return "\\".join(f_split[0:-1]) + "\\"+f_split[-2] + ".csv"

    elif  len(files) > 1:
        content = pd.read_csv(files[0],sep = separator)
        col_names = content.columns.to_list()
        f_split = files[0].split("\\")
        new_file = "\\".join(f_split[0:-1]) + "\\"+f_split[-2] + "_tempfileforimporter.csv"

        i=1
        while i <len(files):
            cont = pd.read_csv(files[i],sep = separator)
            if col_names != cont.columns.to_list():
                return "error"

            else:
                content = pd.concat([content,cont])
            i = i + 1

    content.to_csv(new_file ,index = False,sep=separator)
    return new_file

def gen_sql_file(f_c):
    # generates sql code - if table not exists create table and bulk insert/insert by row based on var insert_mode
    filepath = [c for c in  f_c.keys()][0]
    table_name = os.path.basename(filepath).replace("_tempfileforimporter","").replace(".csv","")
    cols = [c for c in  f_c.values()][0]
    query = SQLTemplates.check_if_table_exists.replace("tablenameplaceholder",table_name)
    query = query + "\n" + "begin" + "\n" + SQLTemplates.create_tabel.replace("tablenameplaceholder",table_name) +"\n" +  "("

    for i,col in enumerate(cols,start =0):

        if i < len(cols)-1:
            query = query +  "\t" + "[" +  list(col)[0] + "] " + col.get(list(col)[0]) +  " null," + "\n"
        else:
             query = query +  "\t" + "[" + list(col)[0] + "]" + " " + col.get(list(col)[0]) + " null \n" + ")" + "\n"


    if insert_mode == "bulk":
        query = query + "\n" + SQLTemplates.bulk_insert.replace("tablenameplaceholder",table_name).replace("filepathplaceholder",filepath).replace("separatorplaceholder",separator)
    elif insert_mode =="row":
        content = pd.read_csv(filepath,sep=";")

        for c in cols:
            if list(c.values())[0] == "int":
                col_name = str(list(c.keys())[0])
                content[col_name] = content[col_name].astype("Int64")

        insert_all_rows =""

        for index,row in content.iterrows():
            insert_r = SQLTemplates.insert_row.replace("tablenameplaceholder",table_name)
            for nr,col_type in enumerate(cols,start=0):
                if list(col_type.values())[0] == "datetime" or "nvarchar" in list(col_type.values())[0]:
                    if str(row[nr]) =="nan":
                        insert_r = insert_r + 'null' + ","
                    else:
                        insert_r = insert_r +"'"+ str(row[nr]) + "',"
                else:
                    insert_r = insert_r + str(row[nr]).replace("<NA>",'null') + ","
            insert_all_rows = insert_all_rows +"\n" + insert_r[0:-1] + ")"

        query = query + insert_all_rows 
        
    query = query + "\n end" + "\n commit tran"

    if os.path.isfile(query_path + "\\" + table_name +".sql"):
        os.remove(query_path + "\\" + table_name +".sql")
    with open(query_path + "\\" + table_name +".sql","w",encoding="utf-8") as f:
        f.write(query)


    return query

