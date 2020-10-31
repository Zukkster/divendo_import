from parse_config import fn_parse_config
from configparser import ConfigParser
import logging
import logging.config
import psycopg2
from os import path

def fn_write_postgres_csv(sql_str,out_file):
  
    conn = None
    try:
        
        log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
        print(log_file_path)
        logging.config.fileConfig(log_file_path)
        logger=logging.getLogger(__name__)
        
        login_params = fn_parse_config("etl_process.ini", "psycopg_login")       
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**login_params)

        cursor = conn.cursor()
        cursor.execute(sql_str)

        logger.info("Write SQL resuts to file")     
        with open(out_file + ".csv", "w") as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow([i[0] for i in cursor.description]) # write headers
            for row in cursor:
                writer.writerow(row)
 
        cursor.close()
        conn.close()
        
    except Exception:
        logger.error("Error in ", exc_info=True)
        #print("error")
    finally:
        if conn is not None:
            conn.close()


#===============================================================================================================


import csv
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

def fn_send_csv_email():
    
        segment_rows=0
        load_rows=0
        
        log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
        print(log_file_path)
        logging.config.fileConfig(log_file_path)
        logger=logging.getLogger(__name__)
    
        email_params = fn_parse_config("etl_process.ini", "email_login") 
        
        #recipients = ['stuartkirkup@gmail.com']
        recipients = ['stuartkirkup@gmail.com', 'james@performalytics.co.uk']

        table_segment = '' 
        with open('output_segment.csv') as csvFile: 
            reader = csv.DictReader(csvFile, delimiter=',')    
            table_segment = '<tr>{}</tr>'.format(''.join(['<td class="cell">{}</td>'.format(header) for header in reader.fieldnames])) 
            for row in reader:  
                table_row = '<tr>' 
                for fn in reader.fieldnames:            
                    table_row += '<td class="cell">{}</td>'.format(row[fn]) 
                table_row += '</tr>' 
                table_segment += table_row
                segment_rows+=1
                
        table_load = '' 
        with open('output_load.csv') as csvFile: 
            reader = csv.DictReader(csvFile, delimiter=',')    
            table_load = '<tr>{}</tr>'.format(''.join(['<td class="cell">{}</td>'.format(header) for header in reader.fieldnames])) 
            for row in reader:  
                table_row = '<tr>' 
                for fn in reader.fieldnames:            
                    table_row += '<td class="cell">{}</td>'.format(row[fn]) 
                table_row += '</tr>' 
                table_load += table_row
                load_rows+=1

        html = """
        <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
        <title></title>
        <style type="text/css" media="screen">
        table{
            background-color: #ffffff;
            empty-cells:hide;
          Border:5px solid red;
         }
         td, th {
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }
        tr:nth-child(even) {
            background-color: #dddddd;
        }

        </style>
        </head>
        <html><body><p>Hi!</p>
        <p>The following files have loaded in the last hour</p>
        <table style="border: black 0.5px"> 
        %s 
        </table>
        <p>The following segment updates have been applied</p>
        <table style="border: black 0.5px"> 
        %s 
        </table>
        <p>Regards,</p>
        </body></html>""" % (table_load, table_segment)

        print(html)
        
        #only send the email if something loaded
        if segment_rows + load_rows > 0:
            message = MIMEMultipart(
                "alternative", None, [MIMEText(html,'html')])
    
    
            message['Subject'] = "LOAD SUMMARY - VIVOBAREFOOT"
            message['From'] = email_params['email']
            message['To'] = ", ".join(recipients)
            server = smtplib.SMTP(email_params['server'])
            server.ehlo()
            server.starttls()
            server.login(email_params['email'],email_params['password'])
            server.sendmail(email_params['email'], recipients, message.as_string())
            server.quit()
        
        #print('checks')
        #print(segment_rows)
        #print(load_rows)


