import requests
import pyodbc
import json
import ftfy # encoding
from flatsplode import flatsplode # flatenize json
import sys
import time 
import pandas as pd
from bcpandas import SqlCreds, to_sql
from datetime import datetime
import gc
from pandas import json_normalize
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import zipfile  
import io
import urllib
import ijson
from ocdskit.combine  import *
import flatterer
import os 
from dotenv import load_dotenv
project_folder = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))
load_dotenv(os.path.join(project_folder, '.env'))

#check if json
def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True

#to_sql, .env required
creds = SqlCreds(
os.getenv("TO_SQL_HOST"), 
os.getenv("TO_SQL_DBSTAGE"), 
os.getenv("TO_SQL_USER"), 
os.getenv("TO_SQL_PWD"), 
)

#endpoints list opensource
listaJson2022 = [

    {
        'nombre':'sefin',
        'anio':'2022',
        'url':'https://piep.sefin.gob.hn/edca/ocid_sefin_2022.zip',
        'desc':'servicio propio de segin.gob.hn de procesos de compra, incluye budget break.'
    }

]


#iterate
for origen in listaJson2022:

    url    = origen['url']
    nombre = origen['nombre']
    anio   = origen['anio']
    tablename = "tmp_"+nombre+"_jsonzip_"+anio

    #get 
    #request
    try:
        print("--> pre_request: {}".format(datetime.now()))
        print('url:'+url)
        access_url = urllib.request.urlopen(url,timeout=60)
        z = zipfile.ZipFile(io.BytesIO(access_url.read()))
        z.extractall()        

        # Opening JSON file
        if anio == '2022':
            jsonfile = 'ocid_sefin_'+anio+'.json'
        else:
            jsonfile = 'EDCA/ocid_sefin_'+anio+'.json'

        with open(jsonfile, 'r', encoding="utf-8") as fp:
            countLines = sum(1 for _ in fp)
            print(countLines)

        # split into parts by countlines (memory issue fix)
        if countLines > 20000:
            splits = 20
        else:
            splits = 10
        num, div = countLines, splits     
        parts = [num // div + (1 if x < num % div else 0)  for x in range (div)]
        print(parts)

        desde = 0
        hasta = 0

        for index,part in enumerate(parts):

            i=1 #record
            dfmain = pd.DataFrame()

            # desde, hasta
            if index > 0:
                desde = sum(parts[:index])
                hasta = sum(parts[:index+1])
            else:
                desde = 0
                hasta = part
            
            with open(jsonfile, 'r', encoding="utf-8") as fp:                
                for line in fp.readlines()[desde:hasta]:

                    if is_json(line):
                            
                        line = json.loads(line)
                        releases = line['releases']

                        #merger OCDS for compiled release
                        ##merge ocdskit
                        import ocdsmerge
                        merger = ocdsmerge.Merger()
                        compiled_release = merger.create_compiled_release(releases)

                        if len(str(compiled_release)) < 50000: 

                            #flatenize with flatsplode
                            dfnew = pd.DataFrame(list(flatsplode(compiled_release))).replace('\\t',' ', regex=True) 

                            # columns
                            dfnew.columns = dfnew.columns.str.strip().str.lower().str.replace(' ', '_', regex=True).str.replace('(', '', regex=True).str.replace(')', '', regex=True).str.replace('/', '_', regex=True)
                            
                            # delimiter chars
                            dfnew = dfnew.replace('\\t','', regex=True).replace('|','', regex=True).replace(';','', regex=True).replace(',','', regex=True)

                            # quote chars
                            dfnew = dfnew.replace('#','', regex=True).replace('~','', regex=True)

                            # to dfmain
                            dfmain = pd.concat([dfmain,dfnew], axis=0, ignore_index=True)

                            # clean mem
                            del dfnew
                            gc.collect()                   

                        print(".", sep=' ', end='', flush=True)
                        if i%20 == 0:
                            print(str(i)+'/'+str(hasta-desde)+' ('+str(countLines)+')')
                        i=i+1

            #insert start
            try:
                print(dfmain.shape)
                print("tables:")
                print(tablename+"_p"+str(index+1))
                #insert with replace into paginated tables
                to_sql(dfmain, tablename+"_p"+str(index+1), creds, index=False, if_exists='replace')
                del dfmain
                gc.collect()                 

            except SystemExit:
                print("--> systemexit except: {}".format(datetime.now()))

            except:
                print('--> insert except')    
                print ( sys.exc_info() )
                sys.exit(1)

    except requests.exceptions.RequestException as e:
        print('--> request except')
        sys.exit(1)         
    print("--> post_request: {}".format(datetime.now()))
