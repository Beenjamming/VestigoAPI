import requests
import json
import pandas as pd
from sqlalchemy import create_engine
PARS = create_engine(f'''mssql+pyodbc://UCSFMC\michaelsb@Pharmacy''')
ClientID = ""
ClientSecret = ""
url = "https://api.vestigo.biz/api/oauth2/token"
r = requests.post(url, data = {'grant_type':'client_credentials','client_id':ClientID,'client_secret':ClientSecret})
access_token = r.json()['access_token']
#print('Bearer {access_token}'.format(access_token=access_token))
for i in range (0,99999999,999):
    payload_hx = {'offset':i,'limit':999,'accountNumber':'','fromServiceDate':'','toServiceDate':'','fromChangeDate':'','toChangeDate':''}
    bcrequest = "https://api.vestigo.biz/api/1/billing-charges"
    my_headers = {'Authorization': 'Bearer {access_token}'.format(access_token=access_token)}
    billingcharges = requests.get(bcrequest, headers=my_headers,params=payload_hx)
    billingcharges.status_code
    rx_json = json.loads(billingcharges.text)
    df = pd.read_json(billingcharges.text)
    if df.empty == True:
        break
    df.to_csv(f'C:\\Users\\MICHAELSB\\Documents\\TEMP\\Hx\\BC_History_{i}.csv',index = False)

import os, glob
from datetime import datetime
t = datetime.today()
for newfile in glob.glob("C:\\Users\\MICHAELSB\\Documents\\TEMP\\Hx\\BC_History_*.csv"): 
    try:
        df = pd.read_csv(newfile)
        print(newfile)
        df['InsertDTTM'] = t
        df.to_sql('BillingCharges', con=PARS,schema='Pharmacy.Vestigo', index=False, if_exists='append')
        print(newfile + ' - Success')
        os.remove(newfile)      
    except:
        print('Error ' + str(newfile))
    
    69930
    131868
