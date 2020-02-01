import logging
import csv
import json
import time
import smtplib,psycopg2,re
from configparser import ConfigParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

class CSVandJSON:

    def __init__(self,CSVfile,JSONfile):
        self.CSVfile=CSVfile
        self.JSONfile=JSONfile
        self.CurrentTime = time.strftime('%Y%m%d%H%M%S')
    def convert(self):
        #read the csv file
        logging.basicConfig(filename='log/'+self.CurrentTime+'.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)
        logging.info('Reading CSV File....')
        with open (self.CSVfile) as csvFile:
            csvReader = csv.DictReader(csvFile)
            rows = list(csvReader)
            logging.basicConfig(filename='log/'+self.CurrentTime+'.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)
            logging.info('Processing CSV File....')
                # write the data to a json file
            logging.basicConfig(filename='log/'+self.CurrentTime+'run.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)
            logging.info('Writing JSON File....')
        with open(self.JSONfile, "w") as jsonFile:
            json.dump(rows, jsonFile, indent=4)

    def synchonize(self):
        logging.basicConfig(filename='log/'+self.CurrentTime+'.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)
        logging.info('Checking CSV File....')
        with open (self.CSVfile) as csvFile:
            csvReader = csv.DictReader(csvFile)
            rows = list(csvReader)
        with open(self.JSONfile) as jsonFile:
            CheckRows = json.load(jsonFile)

        if(CheckRows!=rows):
            logging.basicConfig(filename='log/'+self.CurrentTime+'.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)
            logging.info('Updating JSON File....')
            with open (self.CSVfile) as csvFile:
                csvReader = csv.DictReader(csvFile)
                rows = list(csvReader)
                    # write the data to a json file
            with open(self.JSONfile, "w") as jsonFile:
                json.dump(rows, jsonFile, indent=4)
        else:
            logging.basicConfig(filename='log/'+self.CurrentTime+'.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)
            logging.info('Nothing changed')

    def sendEmail(self):
        parser = ConfigParser()
        parser.read('Config/config.conf')

        emailuser = str(parser.get('mail-config', 'SMTP')  )
        emailpassword = str(parser.get('mail-config', 'username'))
        emailsend = str(parser.get('mail-config', 'password'))

        subject = 'subject'

        msg = MIMEMultipart()
        msg['From'] = emailuser
        msg['To'] = emailsend
        msg['Subject'] = subject

        body = 'Hi there, sending this email from Python!'
        msg.attach(MIMEText(body,'plain'))

        filename='log/20190610182843.log'
        attachment  =open(filename,'rb')

        part = MIMEBase('application','octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',"attachment; filename= "+filename)

        msg.attach(part)
        text = msg.as_string()
        server = smtplib.SMTP('smtp.ethereal.email',587)
        server.starttls()
        server.login(emailuser,emailpassword)


        server.sendmail(emailuser,emailsend,text)
        server.quit()

class ConnectPOSTGRESQL:
    def __init__(self,jsonfile):
        self.jsonfile=jsonfile

    def connect(self):
        try:
            self.connection = psycopg2.connect(user = "postgres",
                                  password = "root",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "filmsdatabase")

        except (Exception, psycopg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)

    def ConvertPostresqltoJson(self):
        cursor = self.connection.cursor()
        cursor.execute("select row_to_json(row) from ( select * from film_list ) row")
        film_list = cursor.fetchall()
        with open(self.jsonfile, "w") as jsonFile:
            for film_list in film_list:
                json.dump(film_list[0],jsonFile,indent=4)

    def relation(self):
        cursor = self.connection.cursor()
        cursor.execute("""select row_to_json(row) from (SELECT title,actors FROM public.nicer_but_slower_film_list) row """)
        film_list = cursor.fetchall()

        for film_list in film_list:
            title = film_list[0]['title']
            film = film_list[0]['actors'].split(',')
            nameArray = list()
            for film in film:
                nameArray.append(re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', film))
            if len(nameArray) != 0:
                actorsIds = list()
                for name in nameArray:
                    cursor = self.connection.cursor()
                    cursor.execute(""" select "actor_info"."actor_id" from "actor_info" where "actor_info"."first_name" = '"""+name[0]+"""' and  "actor_info"."last_name" =  '"""+name[1]+"""' """)
                    filmolist = cursor.fetchall()
                    if len(filmolist) == 0:
                        actorsIds.append([])
                    else:
                        actorsIds.append(filmolist[0][0])
                dict = {}
                dict["title"] = title
                dict["actorIDs"] = actorsIds
                with open(self.jsonfile, "a") as jsonFile:
                    json.dump(dict,jsonFile,indent=4)


        # for name in name:
            # print(name[0]+name[1])
# select row_to_json(row) from ( select * from film_list ) row
# select * from "film_list" where "film_list"."title" = 'Language Cowboy'
# a = CSVandJSON("assets/CSVDemo.csv","assets/CSVDemo.json")
# a.convert()
jsonas = ConnectPOSTGRESQL("assets/CSVDemo.json")
jsonas.connect()
jsonas.relation()
