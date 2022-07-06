# csv_importer

Create new tables and import csv files to Microsoft SQL database.

Notes:
* if error occours on sql side it will be logged in importer.log
* code creates temp files which are deleted after import
* sql types int,decimal,nvarchar based on columns values in csv

Required third-party libraries:
* pandas
* pyodbc

### config_empty.py 
* path - path to single csv file or folder <br>
To import multiple files to one table group them in seprate folder<br>
example:<br><br>
&nbsp;&nbsp;&nbsp;&nbsp; data_to_import\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; sales\\ <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; sales_2021.csv<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; sales_2022.csv<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; clients.csv<br>
In above example 2 tables will be created:<br>
&nbsp;&nbsp;&nbsp;&nbsp; 1. sales with data in sales_2021.csv and sales_2022.csv<br>
&nbsp;&nbsp;&nbsp;&nbsp; 2. clients



* if trusted_connection = True username and password may remain empty<br>
* if save_sql = True - sql statments will be saved in seprate files<br>
Fill variables and rename file to config.py<br>

