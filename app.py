
import os,sys
from functools import wraps
from flask import Flask,current_app,abort,flash,g,redirect,request,render_template, \
                  session,url_for
from flask.ext.wtf import Form,TextField,PasswordField,BooleanField,DateTimeField, \
                          RadioField,SelectField,SelectMultipleField,TextAreaField, \
                          HiddenField,ValidationError,Required,length
from flask.ext.login import LoginManager,login_required,login_user,logout_user, \
                            current_user,fresh_login_required,confirm_login,login_url

import db

# Config
DEBUG = os.environ.get('DEBUG',False)
SECRET_KEY = os.urandom(32)
BOOTSTRAP_USE_CDN = False

# Create App
app = Flask(__name__)
app.config.from_object(__name__)

# Flask-Login
login_manager = LoginManager()
login_manager.setup_app(app)
login_manager.login_view = "login"
login_manager.refresh_view = "refresh"
login_manager.needs_refresh_message = u"Please re-authenticate to access this page"

def admin_required(fn):
    @wraps(fn)
    def decorated_view(*args, **kwargs):
        if not current_user.is_authenticated():
            return current_app.login_manager.unauthorized()
        if not current_user.is_admin():
            flash("Admin login required for this page","error")
            return redirect(login_url(login_manager.login_view,request.url))
        return fn(*args, **kwargs)
    return decorated_view

class User(object):
    def __init__(self,user):
        self._user = user
    def __getattr__(self,k):
        try:
            return self._user[k]
        except KeyError:
            raise AttributeError()
    def get_id(self):
        return unicode(self._user['id'])
    def is_active(self):
        return self.active
    def is_anonymous(self):
        return False
    def is_authenticated(self):
        return True
    def is_admin(self):
        return self.admin

class LoginForm(Form):
    user = TextField('Username', validators=[Required()])
    password = PasswordField('Password', validators=[Required()])
    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False
        user = db.row('users','name',self.user.data)
        print user
        if user is None:
            self.user.errors.append('Unknown User')
            return False
        if self.password.data != user['password']:
            self.password.errors.append('Invalid Password')
            return False
        self.user = User(user)
        return True

@login_manager.user_loader
def load_user(userid):
    user = db.row('users','id',int(userid))
    return User(user) if user else None 

def text(msg,code=200):
    return (msg,code,{'Content-type':'text/plain'})

@app.route('/')
def index():
    return render_template("index.html",message="Hello",user=current_user)

@app.route('/login',methods=('GET','POST',))
def login():
    form = LoginForm()
    if form.validate_on_submit():
        login_user(form.user,remember=True)
        flash("Logged In (%s)" % form.user.name,"success")
        return redirect(request.args.get("next") or url_for("index"))
    return render_template('login.html',login_form=form)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash("Logged Out","success")
    return redirect(url_for("index"))

@app.route('/refresh',methods=('GET','POST',))
def refresh():
    form = LoginForm()
    if form.validate_on_submit():
        confirm_login()
        flash("Authentication refreshed","success")
        return redirect(request.args.get("next") or url_for("index"))
    return render_template('login.html',login_form=form)

@app.route('/admin')
@login_required
@admin_required
def admin():
    return render_template("index.html",message="Admin",user=current_user) 

@app.route('/fresh')
@fresh_login_required
def fresh():
    return render_template("index.html",message="Fresh",user=current_user) 

@app.route('/ping')
@login_required
def ping():
    with db.cursor() as c:
        c.execute('select version()')
        return text(c.fetchone()['version'])

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

