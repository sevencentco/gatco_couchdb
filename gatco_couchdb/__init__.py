from cloudant.client import CouchDB as _CouchDB
from cloudant.database import CouchDatabase as _CouchDatabase
from cloudant.document import Document
from cloudant.design_document import DesignDocument
from .url import URL, make_url

__version__ = '0.1.0'

class _CouchState(object):
    """Remembers configuration for the (db, app) tuple."""

    def __init__(self, db):
        self.db = db
        self.connectors = {}
        
class CouchDatabase(_CouchDatabase):
    def get(self, key, default=None, remote=False):
        """
        Overrides dictionary __getitem__ behavior to provide a document
        instance for the specified key from the current database.
        If the document instance does not exist locally, then a remote request
        is made and the document is subsequently added to the local cache and
        returned to the caller.
        If the document instance already exists locally then it is returned and
        a remote request is not performed.
        A KeyError will result if the document does not exist locally or in the
        remote database.
        :param str key: Document id used to retrieve the document from the
            database.
        :returns: A Document or DesignDocument object depending on the
            specified document id (key)
        """
        if remote is False:
            if key in list(self.keys()):
                return super(CouchDatabase, self).__getitem__(key)
              
        if key.startswith('_design/'):
            doc = DesignDocument(self, key)
        else:
            doc = Document(self, key)
          
        if doc.exists():
            doc.fetch()
            super(CouchDatabase, self).__setitem__(key, doc)
            return doc
          
        return default
        
class CouchDBClient(_CouchDB):
    _DATABASE_CLASS = CouchDatabase
#     
    def __init__(self, *args, **kw):
        super(CouchDBClient, self).__init__(*args, **kw)
        if kw.get('connect', False):
            self.connect()

class CouchDB(object):
    app = None
    client = None
    db = None
    uri = None
    
    def __init__(self, app=None, uri=None):
        self.open_connection = None
        self.close_connection = None
        if app is not None:
            self.init_app(app, uri=uri)
            
    def user_open_connection(self, open_connection):
        """Decorator to set a custom user open_connection"""
        self.open_connection = open_connection
        return open_connection
    
    def user_close_connection(self, close_connection):
        """Decorator to set a custom user open_connection"""
        self.close_connection = close_connection
        return close_connection
    
    def init_app(self, app, name=None, uri=None):
        if uri is None:
            uri = app.config.get('COUCH_DATABASE_URI', None)
            
        if uri is not None:
            self.uri = uri
            url = make_url(uri)
            couch_uri = url.drivername + "://"

            if url.host is not None:
                if ":" in url.host:
                    couch_uri += "[%s]" % url.host
                else:
                    couch_uri += url.host
            if url.port is not None:
                couch_uri += ":" + str(url.port)
            app.config['COUCH_URI'] = couch_uri
            app.config['COUCH_USER'] = url.username
            app.config['COUCH_PASSWORD'] = url.password
            app.config['COUCH_DB'] = url.database

        elif app.config.get('COUCH_URI') is not None:
            self.uri = app.config.get('COUCH_URI')
        else:
            raise RuntimeError('COUCH_URI must be set')
         
        if not app.config.get('COUCH_USER'):
            raise RuntimeError('COUCH_USER must be set')
        
        if not app.config.get('COUCH_PASSWORD'):
            raise RuntimeError('COUCH_PASSWORD must be set')
        
        self.app = app
        self.default_open_connection(app)
        if self.open_connection is not None:
            self.open_connection(app)
            
#         if close_listener:
#             @app.listener(close_listener)
#             async def close_connection(app, loop):
#                 self.default_close_connection(app, loop)
#                 if self.close_connection is not None:
#                     await self.close_connection(app, loop)
        
        if (not hasattr(app, 'extensions')) or (app.extensions is None):
            app.extensions = {}
        app.extensions['couchdb'] = _CouchState(self)
        
    def get_app(self, reference_app=None):
        """Helper method that implements the logic to look up an
        application."""

        if reference_app is not None:
            return reference_app

        if self.app is not None:
            return self.app

        raise RuntimeError(
            'No application found. Either work inside a view function or push'
            ' an application context.'
        )
        
    def default_open_connection(self, app):
        self.client = CouchDBClient(app.config.get('COUCH_USER'),app.config.get('COUCH_PASSWORD'), url=app.config.get('COUCH_URI'), connect=True, auto_renew=True)
        #self.client._DATABASE_CLASS = CouchDatabase
        
        if app.config.get('COUCH_DB') is not None:
            self.db = self.client.create_database(app.config.get('COUCH_DB'), throw_on_exists=False)
        
        
    def default_close_connection(self, app, loop):
        if self.client is not None:
            self.client.close()
    
