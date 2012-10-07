
import logging
import multiprocessing
import multiprocessing.queues
import os.path
import smtplib
import time

from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate,make_msgid,getaddresses,parseaddr
from mimetypes import guess_type
from smtplib import SMTPResponseException,SMTPServerDisconnected

class GMail(object):

    def __init__(self,username,password,debug=False):
        self.server = 'smtp.gmail.com'
        self.port = 587
        self.username = parseaddr(username)[1]
        self.password = password
        self.sender = username
        self.debug = debug
        self.session = None

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
        if not self.is_connected():
            self.connect()
        if rcpt is None:
            rcpt = [ addr[1] for addr in getaddresses((message.get_all('To') or []) + 
                                                      (message.get_all('Cc') or []) + 
                                                      (message.get_all('Bcc') or [])) ]
        if message['From'] is None:
            message['From'] = self.sender
        if message['Reply-To'] is None:
            message['Reply-To'] = self.sender
        if message['Date'] is None:
            message['Date'] = formatdate(time.time(),localtime=True)
        if message['Message-ID'] is None:
            message['Message-ID'] = make_msgid()
        del message['Bcc']

        self.session.sendmail(self.sender,rcpt,message.as_string())

    def is_connected(self):
        if self.session is None:
            return False
        try:
            rcode,msg = self.session.noop()
            if rcode == 250:
                return True
            else:
                self.session = None
                return False
        except (SMTPServerDisconnected,SMTPResponseException):
            self.session = None
            return False
            
class GMailWorker(object):

    def __init__(self,username,password,debug=False):
        def _gmail_worker(username,password,queue,debug=False):
            gmail = GMail(username,password,debug)
            gmail.connect()
            while True:
                try:
                    msg,rcpt = queue.get()
                    if msg == 'QUIT':
                        break
                    gmail.send(msg,rcpt)
                except SMTPServerDisconnected:
                    gmail.connect()
                    gmail.send(msg,rcpt)
                except SMTPResponseException:
                    pass
                except KeyboardInterrupt:
                    break
            gmail.close()
        self.queue = multiprocessing.queues.SimpleQueue()
        self.worker = multiprocessing.Process(target=_gmail_worker,args=(username,password,self.queue,debug))
        self.worker.start()

    def send(self,message,rcpt=None):
        self.queue.put((message,rcpt))

    def quit(self):
        self.queue.put(('QUIT',None))

    def __del__(self):
        self.quit()

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
    return msg

class GMmailHandler(logging.Handler):

    def __init__(self,username,password,to,subject,bg=False):
        logging.Handler.__init__(self)
        if bg:
            self.gmail= GMailWorker(username,password)
        else:
            self.gmail= GMail(username,password)
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
            self.gmail.send(msg)
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
    print gmail.send(msg)
    gmail.close()
