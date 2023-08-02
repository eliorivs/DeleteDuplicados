import datetime
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import mysql.connector
from openpyxl import load_workbook
import xlsxwriter
conn = mysql.connector.connect(host="gpcumplimiento.cl",database="gpcumpli_bombeosld",user="gpcumpli_admin",password="30cuY2[OAgAr", consume_results=True)
nombre_archivo = "datos.txt"

def generar_df():
    conexion = create_engine('mysql+pymysql://root:@localhost:3306/gpcumpli_bombeosld')
    sentencia_sql = "SELECT * FROM mediciones_continuas "
    df = pd.read_sql_query(sentencia_sql, conexion)
    return(df)

def process():

    df = generar_df()
    if (len(df) == 0):
        print ('df is empty')
    else:       
        filtered_df = df[df.duplicated(subset=['Timestamp', 'estacion'], keep=False)]
        df= filtered_df.sort_values(by=['estacion', 'Timestamp'])
        namefile="ordenado.xlsx"
        df.to_excel(namefile, index=False)
        for column in df.columns[5:]:
            df[column] = df[column].combine_first(df.groupby(['estacion', 'Timestamp'])[column].transform('first'))
        df.reset_index(drop=True, inplace=True)
        merged_df = df.drop_duplicates(subset=['estacion', 'Timestamp'], keep='last')   
        merged_df = merged_df.drop_duplicates(subset=['estacion', 'Timestamp'], keep='last')
        namefile="optimizado.xlsx"
        merged_df.to_excel(namefile, index=False)
        return(merged_df)

def eliminar_duplicados(estacion,timestamp):   
   
    try:
        cursor = conn.cursor()  
        sql_query = "DELETE FROM mediciones_continuas WHERE estacion='"+str(estacion)+"' and timestamp='"+str(timestamp)+"'; \n";   
        cursor.execute(sql_query) 
        conn.close()
        return(sql_query)
    except Exception as e:
        print(f"Error inesperado: {e}")

def load_dataframe(df):
   
    try:
        conexion = create_engine('mysql+pymysql://root:@localhost:3306/gpcumpli_bombeosld')
        nombre_tabla = 'mediciones_continuas'  
        df.to_sql(name=nombre_tabla, con=conexion, if_exists='replace', index=False)
        conn.close()
    except Exception as e:
        print(f"Error inesperado: {e}")


df = process()
if (len(df) == 0):
    print ('df is empty')
else:    
        for index, row in df.iterrows():
            estacion = row['estacion']
            timestamp = row['Timestamp']
            print (f'Eliminando {estacion} - {timestamp}')
            eliminar_duplicados(estacion,timestamp)
           
load_dataframe(df)