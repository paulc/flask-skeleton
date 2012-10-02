
import logging
import multiprocessing
import os.path
import smtplib
import time

from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate,make_msgid,getaddresses,parseaddr
from mimetypes import guess_type
#from contextlib import contextmanager

class GMail(object):

    def __init__(self,username,password,debug=False):
        self.server = 'smtp.gmail.com'
        self.port = 587
        self.username = parseaddr(username)[1]
        self.password = password
        self.sender = username
        self.debug = debug

    def connect(self):
        self.session = smtplib.SMTP(self.server,self.port)
        self.session.set_debuglevel(self.debug)
        self.session.ehlo()
        self.session.starttls()
        self.session.ehlo()
        self.session.login(self.username,self.password)

    def close(self):
        self.session.quit()

    def send(self,message,rcpt=None):
        ## TODO Check connection active
        if rcpt is None:
            rcpt = [ addr[1] for addr in getaddresses((message.get_all('To') or []) + 
                                                      (message.get_all('Cc') or []) + 
                                                      (message.get_all('Bcc') or [])) ]
        if message['From'] is None:
            message['From'] = self.sender
        if message['Reply-To'] is None:
            message['Reply-To'] = self.sender
        del message['Bcc']
        self.session.sendmail(self.sender,rcpt,message.as_string())

def message(subject,to,cc=None,bcc=None,text=None,html=None,attachments=None):
    parts = []
    _text = MIMEText(text,'plain','utf-8' if isinstance(text,unicode) else 'us-ascii')
    _html = MIMEText(html,'html','utf-8' if isinstance(html,unicode) else 'us-ascii')
    if html:
        alt = MIMEMultipart('alternative')
        alt.attach(_text)
        alt.attach(_html)
        parts.append(alt)
    else:
        parts.append(_text)
    for a in attachments or []:
        if isinstance(a,MIMEBase):
            parts.append(a)
        else:
            main,sub = (guess_type(a) or ('application/octet-stream',''))[0].split('/',1)
            attachment = MIMEBase(main,sub)
            attachment.set_payload(file(a).read())
            attachment.add_header('Content-Disposition','attachment',filename=os.path.basename(a))
            encode_base64(attachment)
            parts.append(attachment)
    if len(parts) == 0:
        raise ValueError('Empty Message') 
    elif len(parts) == 1:
        msg = parts[0]
    else:
        msg = MIMEMultipart()
        for part in parts:
            msg.attach(part)
    msg['To'] = to
    if cc: msg['Cc'] = cc
    if bcc: msg['Bcc'] = bcc
    msg['Subject'] = subject
    msg['Date'] = formatdate(time.time(),localtime=True)
    msg['Msg-Id'] = make_msgid()
    return msg

class GMmailHandler(logging.Handler):

    def __init__(self,username,password,to,subject):
        logging.Handler.__init__(self)
        self.gmail= GMail(username=username,password=password)
        self.to = to
        self.subject = subject

    def getSubject(self, record):
        return record.levelname + " " + self.subject

    def getText(self,record):
        return str(record)

    def emit(self,record):
        try:
            msg = message(self.getSubject(record),to=self.toaddr,text=self.getText(record))
            msg.body = record.levelname + " " + self.format(record)
            self.gmail.bg_send(msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
        
if __name__ == '__main__':
    import argparse,getpass,mimetypes,sys

    parser = argparse.ArgumentParser(description='Send email message via GMail account')
    parser.add_argument('--username','-u',required=True,
                                help='GMail Username')
    parser.add_argument('--password','-p',default=None,
                                help='GMail Password')
    parser.add_argument('--to','-t',required=True,action='append',default=[],
                                help='Recipient (multiple allowed)')
    parser.add_argument('--subject','-s',required=True,
                                help='Subject')
    parser.add_argument('--body','-b',
                                help='Message Body (text)')
    parser.add_argument('--html','-l',default=None,
                                help='Message Body (html)')
    parser.add_argument('--attachment','-a',action='append',default=[],
                                help='Attachment (multiple allowed)')
    parser.add_argument('--debug','-d',action='store_true',default=False,
                                help='Debug')

    results = parser.parse_args()

    if results.password is None:
        results.password = getpass.getpass("Password:")

    if results.body is None and results.html is None:
        results.body = sys.stdin.read()

    gmail = GMail(username=results.username,
                  password=results.password,
                  debug=results.debug)
    msg = message(subject=results.subject,
                  to=",".join(results.to),
                  text=results.body,
                  html=results.html,
                  attachments=results.attachment)
    gmail.connect()
    gmail.send(msg)
    gmail.close()
