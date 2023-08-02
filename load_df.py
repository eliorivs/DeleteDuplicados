import datetime
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import mysql.connector
from openpyxl import load_workbook
import xlsxwriter
#conn = mysql.connector.connect(host="gpcumplimiento.cl",database="gpcumpli_bombeosld",user="gpcumpli_admin",password="30cuY2[OAgAr", consume_results=True)
df = pd.read_excel('optimizado.xlsx',sheet_name='Sheet1')
print('df is load..')
print(df)
try:
    conexion = create_engine('mysql+pymysql://root:@localhost:3306/gpcumpli_bombeosld')
    nombre_tabla = 'mediciones_continuas'  
    df.to_sql(name=nombre_tabla, con=conexion,if_exists='append',index=False)
    print('data was loaded..')
    
except Exception as e:
        print(f"Error inesperado: {e}")
