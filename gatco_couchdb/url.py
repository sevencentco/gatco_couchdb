import re
from urllib.parse import unquote

class URL(object):
    def __init__(
        self,
        drivername,
        username=None,
        password=None,
        host=None,
        port=None,
        database=None,
        query=None
    ):
        self.drivername = drivername
        self.username = username
        self.password_original = password
        self.host = host
        if port is not None:
            self.port = int(port)
        else:
            self.port = None
        self.database = database

    def __to_string__(self, hide_password=True):
        s = self.drivername + "://"
        if self.username is not None:
            s += _rfc_1738_quote(self.username)
            if self.password is not None:
                s += ":" + (
                    "***" if hide_password else _rfc_1738_quote(self.password)
                )
            s += "@"
        if self.host is not None:
            if ":" in self.host:
                s += "[%s]" % self.host
            else:
                s += self.host
        if self.port is not None:
            s += ":" + str(self.port)
        if self.database is not None:
            s += "/" + self.database
        
        return s
    
    def __str__(self):
        return self.__to_string__(hide_password=False)

    def __eq__(self, other):
        return (
            isinstance(other, URL)
            and self.drivername == other.drivername
            and self.username == other.username
            and self.password == other.password
            and self.host == other.host
            and self.database == other.database
            and self.query == other.query
            and self.port == other.port
        )

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return self.__to_string__()

    def __hash__(self):
        return hash(str(self))


    @property
    def password(self):
        if self.password_original is None:
            return None
        else:
            return str(self.password_original)

    @password.setter
    def password(self, password):
        self.password_original = password

    def get_backend_name(self):
        if "+" not in self.drivername:
            return self.drivername
        else:
            return self.drivername.split("+")[0]

    def get_driver_name(self):
        if "+" not in self.drivername:
            return self.get_dialect().driver
        else:
            return self.drivername.split("+")[1]


    def translate_connect_args(self, names=[], **kw):
        translated = {}
        attribute_names = ["host", "database", "username", "password", "port"]
        for sname in attribute_names:
            if names:
                name = names.pop(0)
            elif sname in kw:
                name = kw[sname]
            else:
                name = sname
            if name is not None and getattr(self, sname, False):
                translated[name] = getattr(self, sname)
        return translated
    

def make_url(name_or_url):
    """Given a string or unicode instance, produce a new URL instance.
    The given string is parsed according to the RFC 1738 spec.  If an
    existing URL object is passed, just returns the object.
    """

    if isinstance(name_or_url, (str,)):
        return _parse_rfc1738_args(name_or_url)
    else:
        return name_or_url


def _parse_rfc1738_args(name):
    pattern = re.compile(
        r"""
            (?P<name>[\w\+]+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>.*))?
            @)?
            (?:
                (?:
                    \[(?P<ipv6host>[^/]+)\] |
                    (?P<ipv4host>[^/:]+)
                )?
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            """,
        re.X,
    )

    m = pattern.match(name)
    if m is not None:
        components = m.groupdict()
        if components["database"] is not None:
            tokens = components["database"].split("?", 2)
            components["database"] = tokens[0]

            if len(tokens) > 1:
                query = {}

                for key, value in util.parse_qsl(tokens[1]):
                    if util.py2k:
                        key = key.encode("ascii")
                    if key in query:
                        query[key] = util.to_list(query[key])
                        query[key].append(value)
                    else:
                        query[key] = value
            else:
                query = None
        else:
            query = None
        components["query"] = query

        if components["username"] is not None:
            components["username"] = _rfc_1738_unquote(components["username"])

        if components["password"] is not None:
            components["password"] = _rfc_1738_unquote(components["password"])

        ipv4host = components.pop("ipv4host")
        ipv6host = components.pop("ipv6host")
        components["host"] = ipv4host or ipv6host
        name = components.pop("name")
        return URL(name, **components)
    else:
        raise exc.ArgumentError(
            "Could not parse rfc1738 URL from string '%s'" % name
        )

def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


def _rfc_1738_unquote(text):
    return unquote(text)


def _parse_keyvalue_args(name):
    m = re.match(r"(\w+)://(.*)", name)
    if m is not None:
        (name, args) = m.group(1, 2)
        opts = dict(util.parse_qsl(args))
        return URL(name, *opts)
    else:
        return None