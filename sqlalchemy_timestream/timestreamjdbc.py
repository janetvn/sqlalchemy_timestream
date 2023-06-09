import os
import re
import traceback

import boto3
import logging

from .base import BaseDialect

from sqlalchemy import exc, util
from sqlalchemy.engine import Engine, reflection, URL
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)
from sqlalchemy.sql.sqltypes import (
    BIGINT,
    BOOLEAN,
    DATE,
    FLOAT,
    INTEGER,
    NULLTYPE,
    STRINGTYPE,
    TIMESTAMP,
)
from urllib.parse import unquote_plus


logger = logging.getLogger(__name__)


class UniversalSet(object):
    """UniversalSet

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""

    def __contains__(self, item):
        return True


class TimestreamDMLIdentifierPreparer(IdentifierPreparer):
    """PrestoIdentifierPreparer

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    reserved_words = UniversalSet()


class TimestreamDDLIdentifierPreparer(IdentifierPreparer):
    def __init__(
        self,
        dialect,
        initial_quote="`",
        final_quote=None,
        escape_quote="`",
        quote_case_sensitive_collations=True,
        omit_schema=False,
    ):
        super(TimestreamDDLIdentifierPreparer, self).__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )


class TimestreamStatementCompiler(SQLCompiler):
    """PrestoCompiler
    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    def visit_char_length_func(self, fn, **kw):
        return "length{0}".format(self.function_argspec(fn, **kw))


class TimestreamTypeCompiler(GenericTypeCompiler):
    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_REAL(self, type_, **kw):
        return "DOUBLE"

    def visit_VARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "VARCHAR")

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIME(self, type_, **kw):
        return "TIME"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_INTERVAL(self, type_, **kwargs):
        return "STRING"

    def visit_TIMESERIES(self, type_, **kwargs):
        return "STRING"

    def visit_UNKNOWN(self, type_, **kwargs):
        return "NULL"

    def visit_ARRAY(self, type_, **kwargs):
        return "ARRAY"

    def visit_CLOB(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_NCLOB(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_CHAR(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_NCHAR(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_NVARCHAR(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_TEXT(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_BLOB(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_BINARY(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_VARBINARY(self, type_, **kw):
        return exc.CompileError("Data type `{0}` is not supported".format(type_))


class TimestreamDDLCompiler(DDLCompiler):
    @property
    def preparer(self):
        return self._preparer

    @preparer.setter
    def preparer(self, value):
        pass

    def __init__(
        self,
        dialect,
        statement,
        schema_translate_map=None,
        compile_kwargs=util.immutabledict(),
    ):
        self._preparer = TimestreamDDLIdentifierPreparer(dialect)
        super(TimestreamDDLCompiler, self).__init__(
            dialect=dialect,
            statement=statement,
            schema_translate_map=schema_translate_map,
            compile_kwargs=compile_kwargs,
        )

    def visit_create_table(self):
        pass


_TYPE_MAPPINGS = {
    "boolean": BOOLEAN,
    "double": FLOAT,
    "integer": INTEGER,
    "bigint": BIGINT,
    "varchar": STRINGTYPE,
    "array": STRINGTYPE,
    "row": STRINGTYPE,
    "date": DATE,
    "time": TIMESTAMP,
    "timestamp": TIMESTAMP,
    "interval": STRINGTYPE,
    "timeseries": STRINGTYPE,
    "unknown": NULLTYPE,
}


class TimestreamJDBCDialect(BaseDialect, DefaultDialect):
    _ENV_AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
    _ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
    _ENV_AWS_REGION = "AWS_REGION"

    jdbc_db_name = "awstimestream"
    jdbc_driver_name = "software.amazon.timestream.jdbc.TimestreamDriver"
    jdbc_driver_version = "2.0.0"
    jdbc_jar_name = "amazon-timestream-jdbc-{}-shaded.jar".format(jdbc_driver_version)

    name = "awstimestream"
    driver = "jdbc"
    preparer = TimestreamDMLIdentifierPreparer
    statement_compiler = TimestreamStatementCompiler
    ddl_compiler = TimestreamDDLCompiler
    type_compiler = TimestreamTypeCompiler
    default_paramstyle = "pyformat"
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_statement_cache = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False

    _pattern_column_type = re.compile(r"^([a-zA-Z]+)($|\(.+\)$)")

    _pattern_region_ = re.compile(r"(?<=Region=)[a-z0-9-]*")

    def initialize(self, connection):
        super(TimestreamJDBCDialect, self).initialize(connection)

    def _raw_connection(self, connection):
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def _assume_role(self, role_arn: str):
        # Call STS client and the assume_role method of the STSConnection object and pass the role ARN and a role
        # session name.
        assumed_role_object = boto3.client("sts").assume_role(
            RoleArn=role_arn, RoleSessionName="SqlAlchemyTimestream"
        )
        # From the response that contains the assumed role, get the temporary credentials that can be used to make
        # subsequent API calls
        credentials = assumed_role_object["Credentials"]
        # Use the temporary credentials that AssumeRole returns to make a
        return credentials

    def _find_jar_path_in_class_path(self):
        try:
            class_path = os.getenv("CLASSPATH")
            paths = class_path.split(":")
            for path in paths:
                if self.jdbc_jar_name in path:
                    return path
        except Exception as e:
            traceback.print_exc()

    def create_connect_args(self, url: URL = None):
        if url is None:
            return

        # dialects expect jdbc url e.g.
        # Connection string format: "jdbcapi+timestream://{aws_access_key_id}:{aws_secret_access_key}@timestream.{region}.amazonaws.com/{database_name}?application_name={application_name}t&use_instance_profile={true}&role_arn=a{quote_plus_role_arn}"
        # Ref: https://docs.aws.amazon.com/timestream/latest/developerguide/JDBC.connection-properties.html
        # Build driver arguments - # get region_name from url.host
        driver_args = {"Region": re.sub(
            r"^timestream\.([a-z0-9-]+)\.amazonaws\.(com|com.cn)$", r"\1", url.host
        )}

        if url.username is not None:
            driver_args["AccessKeyId"] = url.username
        if url.password is not None:
            driver_args["SecretAccessKey"] = url.password

        if url.query.get("role_arn"):
            creds = self._assume_role(role_arn=url.query.get("role_arn"))
            driver_args.update(
                {
                    "AccessKeyId": creds["AccessKeyId"],
                    "SecretAccessKey": creds["SecretAccessKey"],
                    "SessionToken": creds["SessionToken"],
                }
            )

        if url.query.get("use_instance_profile") == "true":
            driver_args["AwsCredentialsProviderClass"] = "InstanceProfileCredentialsProvider"

        driver_jar_path = (
            self._find_jar_path_in_class_path()
            if url.query.get("driver_path") is None
            else url.query.get("driver_path")
        )

        if driver_jar_path is None:
            raise Exception(
                "No JDBC Driver path is provided in the connection string or the environment variable "
                "CLASSPATH"
            )

        kwargs = {
            "jclassname": self.jdbc_driver_name,
            "url": r"jdbc:timestream://",
            # pass driver args via JVM System settings
            "driver_args": driver_args,
            "jars": driver_jar_path,
        }
        return (), kwargs

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = """
                SHOW DATABASES
                """
        return [row.schema_name for row in connection.execute(query).fetchall()]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT TABLES
                FROM {}
                """.format(
            schema
        )
        return [row.table_name for row in connection.execute(query).fetchall()]

    def has_table(self, connection, table_name, schema=None):
        table_names = self.get_table_names(connection, schema)
        if table_name in table_names:
            return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                DESCRIBE {schema}.{table}
                """.format(
            schema=schema, table=table_name
        )
        return [
            {
                "name": row.column_name,
                "type": _TYPE_MAPPINGS.get(
                    self._get_column_type(row.data_type), NULLTYPE
                ),
                "nullable": True if row.is_nullable == "YES" else False,
                "default": row.column_default,
                "ordinal_position": row.ordinal_position,
                "comment": row.comment,
            }
            for row in connection.execute(query).fetchall()
        ]

    def _get_column_type(self, type_):
        return self._pattern_column_type.sub(r"\1", type_)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Timestream has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Timestream has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # Timestream has no support for indexes.
        return []

    def do_rollback(self, dbapi_connection):
        # No transactions for Timestream
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # Requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # Requests gives back Unicode strings
        return True
