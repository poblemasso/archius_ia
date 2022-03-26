import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from datetime import datetime

# Generacio de variables y definir Funcions
meses = ['gener','febrer','març','abril','maig','juny','juliol','agost','setembre','octubre','novembre','desembre']
URL = "https://teatreprincipalinca.com"
dialect='mysql+pymysql://root@localhost:3306/scrapping'
#dialect= "mysql+pymysql://root:Bigdata2122@localhost:3306/scrapping"
usuari= "Biel"
projecte = "Teatre Inca"
codi = [0, 1, 2, 3]
texte = ["Dades inserides sense problema",
       "No hi havia connexió amb la web",
       "No hi havia dades noves per inserir",
       "Revisar web, no es troben les dades"]

eventos = pd.DataFrame()
dtlog = pd.DataFrame()
sqlEngine=create_engine(dialect)
def passDate(fecha):
    for i in meses:
        if i in fecha:
            fecha = fecha.replace(i, str(meses.index(i) + 1))
    return fecha 

#Inici WebScrapping 
try:
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    job_elements = soup.find_all("div", class_="hps-item")
    
except:
    dtlog = dtlog.append({
        'usuari': usuari,
        'data': datetime.now(),
        'codi': codi[1],
        'texte': texte[1],
        'projecte': projecte
    }, ignore_index=True)
else:
    try:
        for job_element in job_elements:
            tipo = [s.text for s in job_element.find('h6').find_all('span')]
            fecha, loc = job_element.find('div', class_='hpsi-text').find_all('span')[-1].text.split(' · ')
            titulo = job_element.find('h4').text
            precios = BeautifulSoup(requests.get(URL + job_element.find("a")["href"]).content, "html.parser").find_all('span')
            precio = [p.text.replace('Preu ', '').replace('€', '').split(', ') for p in precios if '€' in p.text]
            eventos = eventos.append({
              'tipus': ' '.join(tipo),
              'data': datetime.strptime(passDate(fecha), '%d %m %Y %H:%M'),
              'sala': loc,
              'titol': titulo,
              'preu': precio
            }, ignore_index=True)
        precios = eventos[['preu', 'titol']]
        precios = precios.explode('preu').explode('preu')
        precios['preu'] = precios['preu'].fillna(0)
        precios['preu'] = [str(p).replace(',', '.') for p in precios['preu']]
        eventos = eventos.fillna('')
        eventos = eventos[['titol', 'data', 'sala', 'tipus']]
        precios = precios[['titol', 'preu']]
    except:
        dtlog = dtlog.append({
            'usuari': usuari,
            'data': datetime.now(),
            'codi': codi[3],
            'texte': texte[3],
            'projecte': projecte
        }, ignore_index=True)
    else:
        coun = list(sqlEngine.connect().execute("SELECT count(*) from gabrielriutort_eventsinca"))[0][0]
        query_eventos = str(f"""INSERT IGNORE INTO gabrielriutort_eventsinca VALUES {','.join([str(i) for i in list(eventos.to_records(index=False))])}""")
        sqlEngine.connect().execute(query_eventos)
        query_precios = str(f"""INSERT IGNORE INTO gabrielriutort_eventspreu VALUES {','.join([str(i) for i in list(precios.to_records(index=False))])}""")
        sqlEngine.connect().execute(query_precios)
        fincoun = list(sqlEngine.connect().execute("SELECT count(*) from gabrielriutort_eventsinca"))[0][0]
        if fincoun != coun:
            dtlog = dtlog.append({
                'usuari': usuari,
                'data': datetime.now(),
                'codi': codi[0],
                'texte': texte[0],
                'projecte': projecte
            }, ignore_index=True)
        else:
            dtlog = dtlog.append({
                'usuari': usuari,
                'data': datetime.now(),
                'codi': codi[2],
                'texte': texte[2],
                'projecte': projecte
            }, ignore_index=True)
finally:
    dtlog[['usuari', 'data', 'codi', 'texte', 'projecte']].to_sql('datalog', con=sqlEngine, if_exists='append')
#Final infromant de com ha anat l'execucio a datalog 