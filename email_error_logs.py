from parse_config import fn_parse_config
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

def fn_email_error_logs():
    
    error_text = ""
    append_error = False
    i=0
    
    email_params = fn_parse_config("etl_process.ini", "email_login")
    recipients = ['stuartkirkup@gmail.com']
    #recipients = ['stuartkirkup@gmail.com', 'james@performalytics.co.uk']

    with open("./log/etl_process.log", 'r') as f:
        for line in f:
            i=i+1
            if "ERROR:" in line:
                error_text = error_text + line
                append_error = True
            elif "DEBUG:" in line:
                append_error = False
            elif "INFO:" in line:
                append_error = False
            elif append_error == True:
                error_text = error_text + line

    if len(error_text) > 0:
        print("TEST:" + error_text)
        message = MIMEText(error_text)

        message['Subject'] = "LOAD ERROR - VIVOBAREFOOT"
        message['From'] = email_params['email']

        message['To'] = ", ".join(recipients)
        server = smtplib.SMTP(email_params['server'])
        server.ehlo()
        server.starttls()
        server.login(email_params['email'],email_params['password'])
        server.sendmail(email_params['email'], recipients, message.as_string())
        server.quit()
    else:
        print("OK")
        
#fn_email_error_logs()