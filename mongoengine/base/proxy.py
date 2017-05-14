import lazy_object_proxy


class DocumentProxy(lazy_object_proxy.Proxy):
    id = None
    def __init__(self, wrapped, id):
        super(DocumentProxy, self).__init__(wrapped)
        self.id = id