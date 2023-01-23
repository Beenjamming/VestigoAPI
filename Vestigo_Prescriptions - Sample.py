import requests
import json
import pandas as pd
from datetime import datetime
from datetime import timedelta 
from sqlalchemy import create_engine
PARS = create_engine(f'''insertSQLconnectionstringhere''') #update with you SQL connection string
ClientID = "yourclientidhere" #update with your client id
ClientSecret = "yourclientsecrethere" #update with your client secret
url = "https://api.vestigo.biz/api/oauth2/token"
r = requests.post(url, data = {'grant_type':'client_credentials','client_id':ClientID,'client_secret':ClientSecret})
access_token = r.json()['access_token']
d = datetime.utcnow().isoformat()
d1 = (datetime.utcnow()- timedelta(days=8)).isoformat()
d2 = (datetime.utcnow()- timedelta(days=1)).isoformat()
fromChangeDate = d1
toChangeDate = d2
payload = {'offset':0,'limit':999,'protocolNumber':'','fromServiceDate':'','toServiceDate':'','fromChangeDate':fromChangeDate,'toChangeDate':toChangeDate}
#payload_hx = {'offset':0,'limit':999,'protocolNumber':'','fromServiceDate':'','toServiceDate':'','fromChangeDate':'','toChangeDate':''}
rxrequest = "https://api.vestigo.biz/api/1/prescriptions"#?offset=0&limit=1000&protocolNumber=&fromServiceDate=&toServiceDate=&fromChangeDate=&toChangeDate="
my_headers = {'Authorization': 'Bearer {access_token}'.format(access_token=access_token)}
rx = requests.get(rxrequest, headers=my_headers,params=payload)
rx.status_code
rx_json = json.loads(rx.text)
df = pd.read_json(rx.text)
t = datetime.today()
try:
    df['InsertDTTM'] = t
    df.to_sql('Prescriptions', con=PARS,schema='Pharmacy.Vestigo', index=False, if_exists='append')  
except:
    import smtplib 
    from email.message import EmailMessage
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    fromaddr = "sendfromemailhere" #update with your email address
    toaddr= ['youremailhere'] #update with your email address
    msg = MIMEMultipart()
    msg['From']= fromaddr
    msg['To']= ", ".join(toaddr)
    msg['Subject']= "Vestigo Prescription History Error" 
    body = "Vestigo API Load failed."
    msg.attach(MIMEText(body,'plain'))
    p = MIMEBase('application', 'octet-stream')
    msg.attach(p)
    text = msg.as_string()
    host = 'smtp.ucsf.edu'
    port = 25
    server = smtplib.SMTP(host, port=port)
    server.sendmail(fromaddr,toaddr,text)
    server.quit()    