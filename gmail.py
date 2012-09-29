
from flask_mail import Mail,Message,email_dispatched

class GMailServer(Mail):

    def __init__(self,username,password,listener=None,debug=False):
        self.app = None
        self.server = 'smtp.gmail.com'
        self.username = username
        self.password = password
        self.port = 587
        self.use_tls =True
        self.use_ssl = False
        self.debug = debug
        self.max_emails = 0
        self.suppress = False
        self.fail_silently = False

        if listener:
            email_dispatched.connect(listener)

if __name__ == '__main__':
    import argparse,getpass,sys

    parser = argparse.ArgumentParser(description='Send email message via GMail account')
    parser.add_argument('--username','-u',required=True,
                                help='GMail Username')
    parser.add_argument('--password','-p',default=None,
                                help='GMail Username')
    parser.add_argument('--from','-f',dest='_from',
                                help='From (default: username')
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

    server = GMailServer(results.username,results.password,debug=results.debug)
    msg = Message(results.subject,results.to,results.body,results.html,results._from)
    server.send(msg)

