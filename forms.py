
from flask.ext.wtf import Form,TextField,PasswordField,BooleanField,DateTimeField, \
                          RadioField,SelectField,SelectMultipleField,TextAreaField, \
                          HiddenField,ValidationError,Required,length
import db
from user import User

class LoginForm(Form):
    user = TextField('Username', validators=[Required()])
    password = PasswordField('Password', validators=[Required()])
    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False
        user = db.select_one('users',{'name':self.user.data})
        print user
        if user is None:
            self.user.errors.append('Unknown User')
            return False
        if self.password.data != user['password']:
            self.password.errors.append('Invalid Password')
            return False
        self.user = User(user)
        return True

