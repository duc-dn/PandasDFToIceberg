from pyiceberg.catalog.hive import HiveCatalog


class HiveCatalogIceberg():
    def __init__(self, catalog_name, host, port, s3_endpoint, s3_access_key_id, s3_secret_access_key):
        self.catalog_name = catalog_name
        self.host = host
        self.port = port
        self.s3_endpoint = s3_endpoint
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.catalog = self._get_catalog()

    def _get_catalog(self):
        catalog = HiveCatalog(
            self.catalog_name,
            **{
                "uri": f"thirft://{self.host}:{self.port}",
                "s3.endpoint": self.s3_endpoint,
                "s3.access-key-id": self.s3_access_key_id,
                "s3.secret-access-key": self.s3_secret_access_key,
            },
        )
        return catalog
    
    def _get_namespace(self):
        return self.catalog.list_namespaces()
