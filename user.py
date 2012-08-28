
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

