import os
import sqlalchemy as db
from sqlalchemy import *
import random
from bs4 import BeautifulSoup
import requests
import time
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import warnings
import csv
import re

warnings.filterwarnings('ignore')
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option('display.max_colwidth', 300)
# set seed for reproducibility
np.random.seed(0)

response = requests.get("https://ycharts.com/companies/TSLA/revenues")

if response.status_code == 200:
    print("The web page responded with status code 200 (OK)")
else:
    print(f"The web page responded with status code {response.status_code}")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

for i in range(5):
    try:
        response = requests.get('https://ycharts.com/companies/TSLA/revenues', headers=headers)
        #response.raise_for_status()
        datos_htlm=response.text
        soup = BeautifulSoup(datos_htlm, 'html.parser')
        with open("tesla_rev.html", "wb") as dataset:
            dataset.write(response.content)
        break
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Request failed with status code {e.response.status_code}. Retrying...")
            headers['User-Agent'] = headers['User-Agent'].replace('Chrome/58.0.3029.110', f'Chrome/{random.randint(50, 60)}.{random.randint(0, 100)}.{random.randint(0, 100)}')
        else:
            raise

tablas_s = soup.find_all("table")

# Encontrar todas las filas de las tablas usando 'tr' tag
rows = soup.find_all('tr')

# Creando una lista de listas que contienen la data de cada columna
data = []
for row in rows:
    cols = row.find_all('td')   # Encuentra las columnas que tienen etiquetas
    row_data = [col.text for col in cols]
    data.append(row_data)

# Vamos a enviar la lista a un archivo .csv para poder abrirlo con PANDAS aprovechando su versatilidad

with open('data_prov.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

Revenue1 = pd.read_csv('C:/Users/gfern/OneDrive/Escritorio/4GeeksAcademy/web-scraping-project-tutorial/src/data_prov.csv',names=['Fecha','Valores'],nrows=50 )
Revenue1['Valores']=Revenue1['Valores'].str.strip()
Revenue1['Fecha_Analizada']=pd.to_datetime(Revenue1['Fecha'],format="%B %d, %Y")
print(Revenue1)

tablas=pd.read_html('C:/Users/gfern/OneDrive/Escritorio/4GeeksAcademy/web-scraping-project-tutorial/src/tesla_rev.html')
for i,tabla in enumerate(tablas):
    tabla.to_csv(f'tabla_{i}.csv', index=True)
print(f'Se encontraron {i+1} tablas a traves del Web Scraping, fueron extraidas con PANDAS')

df1=pd.read_csv('C:/Users/gfern/OneDrive/Escritorio/4GeeksAcademy/web-scraping-project-tutorial/src/tabla_1.csv',usecols=["Date","Value"], nrows=26)
df2=pd.read_csv('C:/Users/gfern/OneDrive/Escritorio/4GeeksAcademy/web-scraping-project-tutorial/src/tabla_0.csv',usecols=["Date","Value"],nrows=26)
Revenue=pd.concat([df1,df2], ignore_index=True)
print(Revenue)

Revenue['Fecha_Analizada'] = pd.to_datetime(Revenue['Date'], format="%B %d, %Y")
Revenue['Value'] = Revenue['Value'].str.strip()
Revenue2=Revenue.sort_values(by=['Fecha_Analizada'])
Revenue2=Revenue2.reset_index()
Revenue2=Revenue2.drop(['index'],axis=1)
Revenue2['Value_Num']=Revenue2['Value']
print(Revenue2)

val=(Revenue2['Value_Num'].dtype)

if val=='object':
  for index in range(len(Revenue2)):
        
    if "M" in Revenue2.loc[index,'Value']:
        Revenue2.loc[[index],'Value_Num']=Revenue2.loc[[index],'Value_Num'].apply(lambda x:re.sub(r'M','',x))
        Revenue2.loc[[index],'Value_Num']=Revenue2.loc[[index],'Value_Num'].astype(float)
        Revenue2.loc[[index],'Value_Num']=Revenue2.loc[[index],'Value_Num'].apply(lambda x:x*1000000)
    
    if "B" in Revenue2.loc[index,'Value']:
        Revenue2.loc[[index],'Value_Num']=Revenue2.loc[[index],'Value_Num'].apply(lambda x:re.sub(r'B','',x))
        Revenue2.loc[[index],'Value_Num']=Revenue2.loc[[index],'Value_Num'].astype(float)
        Revenue2.loc[[index],'Value_Num']=Revenue2.loc[[index],'Value_Num'].apply(lambda x:x*1000000000)

Revenue2['Value_Num']=Revenue2['Value_Num'].astype(float)
print(Revenue2)

engine = db.create_engine('sqlite:///tesla_revenues.sqlite')
connect=engine.connect()
connect.close()

metadata=db.MetaData()
revenues_final = db.Table('revenues_final', metadata,
              db.Column('Fecha', db.String(10),nullable=False),
              db.Column('Valores', db.Float, nullable=False),
              )
metadata.create_all(engine) 

tabla_f = Table('revenues_final', metadata, autoload_with=engine)
with engine.connect() as conn:
        result_f = engine.connect().execute(tabla_f.select())
        if not result_f.fetchall():
                datos_f=[{"Fecha": "31/12/2011", "Valores": 39380000},
                       {"Fecha": "31/03/2012", "Valores": 30170000},
                       {"Fecha": "30/06/2012", "Valores": 26650000},
                       {"Fecha": "30/09/2012", "Valores": 50100000},
                       {"Fecha": "31/12/2012", "Valores": 306330000},
                       {"Fecha": "31/03/2013", "Valores": 561790000},
                       {"Fecha": "30/06/2013", "Valores": 405140000},
                       {"Fecha": "30/09/2013", "Valores": 431350000},
                       {"Fecha": "31/12/2013", "Valores": 615220000},
                       {"Fecha": "31/03/2014", "Valores": 620540000},
                        {"Fecha": "30/06/2014", "Valores": 769350000},
                        {"Fecha": "30/09/2014", "Valores": 851800000},
                        {"Fecha": "31/12/2014", "Valores": 956660000},
                        {"Fecha": "31/03/2015", "Valores": 939880000},
                        {"Fecha": "30/06/2015", "Valores": 954980000},
                        {"Fecha": "30/09/2015", "Valores": 936790000},
                        {"Fecha": "31/12/2015", "Valores": 1214000000},
                        {"Fecha": "31/03/2016", "Valores": 1147000000},
                        {"Fecha": "30/06/2016", "Valores": 1270000000},
                        {"Fecha": "30/09/2016", "Valores": 2298000000},
                        {"Fecha": "31/12/2016", "Valores": 2285000000},
                        {"Fecha": "31/03/2017", "Valores": 2696000000},
                        {"Fecha": "30/06/2017", "Valores": 2790000000},
                        {"Fecha": "30/09/2017", "Valores": 2985000000},
                        {"Fecha": "31/12/2017", "Valores": 3288000000},
                        {"Fecha": "31/03/2018", "Valores": 3409000000},
                        {"Fecha": "30/06/2018", "Valores": 4002000000},
                        {"Fecha": "30/09/2018", "Valores": 6824000000},
                        {"Fecha": "31/12/2018", "Valores": 7226000000},
                        {"Fecha": "31/03/2019", "Valores": 4541000000},
                        {"Fecha": "30/06/2019", "Valores": 6350000000},
                        {"Fecha": "30/09/2019", "Valores": 6303000000},
                        {"Fecha": "31/12/2019", "Valores": 7384000000},
                        {"Fecha": "31/03/2020", "Valores": 5985000000},
                        {"Fecha": "30/06/2020", "Valores": 6036000000},
                        {"Fecha": "30/09/2020", "Valores": 8771000000},
                        {"Fecha": "31/12/2020", "Valores": 10740000000},
                        {"Fecha": "31/03/2021", "Valores": 10390000000},
                        {"Fecha": "30/06/2021", "Valores": 11960000000},
                        {"Fecha": "30/09/2021", "Valores": 13760000000},
                        {"Fecha": "31/12/2021", "Valores": 17720000000},
                        {"Fecha": "31/03/2022", "Valores": 18760000000},
                        {"Fecha": "30/06/2022", "Valores": 16930000000},
                        {"Fecha": "30/09/2022", "Valores": 21450000000},
                        {"Fecha": "31/12/2022", "Valores": 24320000000},
                        {"Fecha": "31/03/2023", "Valores": 23330000000},
                        {"Fecha": "30/06/2023", "Valores": 24930000000},
                        {"Fecha": "30/09/2023", "Valores": 23350000000},
                        {"Fecha": "31/12/2023", "Valores": 25170000000},
                        {"Fecha": "31/03/2024", "Valores": 21300000000},
                ]
                stmt_f = tabla_f.insert().values([dict(row) for row in datos_f])
                conn.execute(stmt_f)
                conn.commit()

res = engine.connect().execute(tabla_f.select())
for row in res:
        print(row)         

connection = engine.connect()
query = 'SELECT * FROM revenues_final'
df = pd.read_sql(query, connection)
print(df)
connection.close()

plt.plot(Revenue2["Fecha_Analizada"],Revenue2['Value_Num'])
plt.title('Empresa Tesla. Crecimiento Trimestral Revenues')
plt.xlabel('Años')
plt.ylabel('Revenue')
plt.show()

Revenue2['Year'] = Revenue2['Fecha_Analizada'].dt.year
revenue_year=Revenue2.groupby(['Year'])['Value_Num'].sum()
lst=revenue_year.tolist()
etiquetas=[]
for i in range(len(lst)):
    etiquetas.append(str(2011+i))

labels = etiquetas
sizes = lst
plt.pie(sizes, labels=labels,autopct='%1.1f%%')
plt.title('TESLA Revenues. Ult Tri 2011 - Pri Tri 2024')
plt.show()

fecha_filter=Revenue2[Revenue2['Fecha_Analizada']>'31/12/2020']
fecha=fecha_filter['Fecha_Analizada'].dt.strftime('%m-%Y').tolist()
valor=fecha_filter['Value_Num'].tolist()

categories = fecha
values = valor

# create a bar chart
plt.bar(categories, values,width=0.8)

# add a title and labels
plt.title('Tesla. Revenues. Evolucion Trimestral 2021 - 2024')
plt.xlabel('Revenues')
plt.ylabel('Trimestre')
plt.xticks(fontsize=6)

# show the plot
plt.show()