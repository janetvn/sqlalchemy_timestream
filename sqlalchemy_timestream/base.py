class BaseDialect(object):
    jdbc_db_name = None
    jdbc_driver_name = None
    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True
    description_encoding = None

    @classmethod
    def import_dbapi(cls):
        import jaydebeapi

        return jaydebeapi

    def is_disconnect(self, e, connection, cursor):
        if not isinstance(e, self.import_dbapi.ProgrammingError):
            return False
        e = str(e)
        return "connection is closed" in e or "cursor is closed" in e

    def do_rollback(self, dbapi_connection):
        pass
