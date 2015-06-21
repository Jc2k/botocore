
from botocore import session
from .client import ClientCreator


class Session(session.Session):

    def _register_client_creator(self):
        self._components.register_component('client_creator_class', ClientCreator)


def get_session(env_vars=None):
    """
    Return a new session object.
    """
    return Session(env_vars)
