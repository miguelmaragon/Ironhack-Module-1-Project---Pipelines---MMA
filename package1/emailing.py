import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import email

import os
from dotenv import load_dotenv

load_dotenv()


def send(archive):
    gmail_user = os.environ["email"]
    gmail_password = os.environ["emailPassword"]

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_password)
        print("Connected to gmail servers")
    except:
        print("Something went wrong...")

    from_mail = gmail_user
    to = input("Which email do you want to send the report to?:  ")
    html = "Here you have the report generated from the data analysis of Forbes Billionaires 2018"
    msg = MIMEMultipart('mixed')

    msg['Subject'] = "Report Forbes Billionaires 2018"
    msg['From'] = from_mail
    msg['To'] = to
    html_contents = MIMEText(html, 'html')

    filename = archive
    fo = open(filename, 'rb')
    attach = email.mime.application.MIMEApplication(fo.read(), _subtype="pdf")
    fo.close()

    attach.add_header('Content-Disposition', 'attachment', filename='Report of Forbes Billionaires 2018')
    msg.attach(attach)
    msg.attach(html_contents)
    server.sendmail(msg['From'], msg['To'], msg.as_string())

    print('Email sent')
    server.close()