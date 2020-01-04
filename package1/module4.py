import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename
import email
import re
import os
from dotenv import load_dotenv

load_dotenv()


def send(archive):
    if not "emailPassword" in os.environ:
        raise ValueError("You should pass a email password")

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
    to = input("Who should receive the mail?:  ")
    while not re.match(r"[^@]+@[^@]+\.[^@]+", to):
        print("Invalid email...")
        to = input("Try again:  ")
    print('Sending email to {}'.format(to))
    html = "Hello, here you have the inform generated from the data analysis of suicides. Enjoy it!"
    msg = MIMEMultipart('mixed')
    msg['Subject'] = "Pipelines Project Inform"
    msg['From'] = from_mail
    msg['To'] = to
    HTML_Contents = MIMEText(html, 'html')
    filename = archive
    fo = open(filename, 'rb')
    attach = email.mime.application.MIMEApplication(fo.read(), _subtype="pdf")
    fo.close()
    attach.add_header('Content-Disposition', 'attachment', filename='Pipeline project Inform')
    msg.attach(attach)
    msg.attach(HTML_Contents)
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    print('Look in your email inbox, there you have!')
    server.close()
