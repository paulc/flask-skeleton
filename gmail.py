
import logging,multiprocessing

from flask_mail import Mail,Message,email_dispatched

class GMail(Mail):

    def __init__(self,app=None,**kwargs):
        if app is None:
            self.init_standalone(**kwargs)
        else:
            # GMail settings
            app.config.update(
                    MAIL_SERVER  = 'smtp.gmail.com',
                    MAIL_PORT    = 587,
                    MAIL_USE_TLS = True,
                    MAIL_USE_SSL = False
            )
            self.init_app(app)

    def init_standalone(self,username,password,listener=None,debug=False,fail=False,suppress=False):
        self.app = None
        self.server = 'smtp.gmail.com'
        self.username = username
        self.password = password
        self.port = 587
        self.use_tls = True
        self.use_ssl = False
        self.debug = debug
        self.max_emails = 0
        self.suppress = suppress
        self.fail_silently = fail

        if listener:
            email_dispatched.connect(listener)

    def bg_send(self,msg):
        p = multiprocessing.Process(target=self.send, args=(msg,))
        p.start()
        return p.pid

class GMmailHandler(logging.Handler):

    def __init__(self,username,password,toaddr,subject):
        logging.Handler.__init__(self)
        self.gmail= GMail(username=username,password=password)
        self.sender = username
        self.toaddr = toaddr
        self.subject = subject

    def getSubject(self, record):
        return self.subject

    def emit(self,record):
        try:
            msg = Message(self.getSubject(record),sender=self.sender,recipients=self.toaddr)
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
                                help='GMail Username')
    parser.add_argument('--from','-f',dest='_from',
                                help='From (default: username)')
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

    if results._from is None:
        results._from = results.username

    if results.body is None and results.html is None:
        results.body = sys.stdin.read()

    server = GMail(username=results.username,
                   password=results.password,
                   debug=results.debug)
    msg = Message(subject=results.subject,
                  recipients=results.to,
                  body=results.body,
                  html=results.html,
                  sender=results._from)
    for f in results.attachment:
        msg.attach(filename=f,
                   content_type=mimetypes.guess_type(f)[0] or 'application/octet-stream',
                   data=file(f).read())
    server.send(msg)
