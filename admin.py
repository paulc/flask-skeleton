
from functools import wraps
from flask import current_app,abort,flash,request,redirect
from flask.ext.login import current_user,login_url,user_unauthorized

def admin_required(fn):
    """
        Extend Flask-Login to support @admin_required decorator
        Requires User class to support is_admin() method
    """
    @wraps(fn)
    def decorated_view(*args, **kwargs):
        if not current_user.is_authenticated():
            return current_app.login_manager.unauthorized()
        try:
            if current_user.is_admin():
                return fn(*args, **kwargs)
        except AttributeError:
            pass
        user_unauthorized.send(current_app._get_current_object())
        flash("Admin login required for this page","error")
        return redirect(login_url(current_app.login_manager.login_view,request.url))
    return decorated_view

