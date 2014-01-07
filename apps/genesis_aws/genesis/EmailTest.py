'''
Created on Jun 20, 2013

@author: masinoa
'''

#This is a simple script that you can use to test the email settings in notifications.conf to ensure an 
#email is sent by the genesis applicaiton

import smtplib
from email.mime.text import MIMEText

def main():
    #UPDATE THESE VALUES
    host = 'smtp.lavabit.com'
    port = 587
    password = '8aFZ5s#!JjmkuQe2'
    sender = 'genesis_app@lavabit.com'
    recepient = 'aaronmasino@gmail.com'
    
    #LEAVE THIS AS IS
    msg = MIMEText("This is a test message")
    msg['Subject'] = "Test"
    msg['From'] = sender
    msg['To'] = recepient
    server = smtplib.SMTP(host, port)
    server.set_debuglevel(1)
    server.ehlo()
    server.starttls()
    server.login(sender, password)
    server.sendmail(sender, recepient, msg.as_string())
    server.quit()
    

if __name__ == '__main__':
    main()