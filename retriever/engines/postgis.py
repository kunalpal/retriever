from __future__ import print_function

import os
from retriever.engines.postgres import engine as PostgreSQLEngine
from retriever.lib.models import no_cleanup
from retriever import ENCODING


class engine(PostgreSQLEngine):
    """Engine instance for PostgreSQL."""
    name = "PostGIS"
    abbreviation = "postgis"
    datatypes = {
        "auto": "serial",
        "int": "integer",
        "bigint": "bigint",
        "double": "double precision",
        "decimal": "decimal",
        "char": "varchar",
        "bool": "boolean",
    }
    max_int = 2147483647
    required_opts = [("user",
                      "Enter your PostgreSQL username",
                      "postgres"),
                     ("password",
                      "Enter your password",
                      ""),
                     ("host",
                      "Enter your PostgreSQL host",
                      "localhost"),
                     ("port",
                      "Enter your PostgreSQL port",
                      5432),
                     ("database",
                      "Enter your PostgreSQL database name",
                      "postgres"),
                     ("database_name",
                      "Format of schema name",
                      "{db}"),
                     ("table_name",
                      "Format of table name",
                      "{db}.{table}"),
                     ]

    def create_db_statement(self):
        """In PostgreSQL, the equivalent of a SQL database is a schema."""
        db_statement = PostgreSQLEngine.create_db_statement(self)
        if not hasattr(self.script, "spatial"):
            print("Tabular data. Falling back to PostgreSQL engine.\n")
            return db_statement
        else:
            try:
                self.check_postgis_availability()
            except Exception as e:
                print("Error: " + e)
                raise
            return ";".join([db_statement]
                        + ["CREATE EXTENSION IF NOT EXISTS {};".format(extension)
                           for extension in ["postgis", "postgis_sfcgal", "postgis_topology"]])

    def create_table(self):
        """PostgreSQL needs to commit operations individually."""
        return PostgreSQLEngine.create_table(self)

    def escape_single_quotes(self, value):
        """Escapes single quotes in the value"""
        return PostgreSQLEngine.escape_single_quotes(self, value)

    def insert_data_from_file(self, filename):
        """Use PostgreSQL's "COPY FROM" statement to perform a bulk insert."""
#         self.get_cursor()
#         ct = len([True for c in self.table.columns if c[1][0][:3] == "ct-"]) != 0
#         if (([self.table.cleanup.function, self.table.delimiter,
#               self.table.header_rows] == [no_cleanup, ",", 1])
#             and not self.table.fixed_width
#             and not ct
#             and (not hasattr(self.table, "do_not_bulk_insert") or not self.table.do_not_bulk_insert)):
#             columns = self.table.get_insert_columns()
#             filename = os.path.abspath(filename)
#             statement = """
# COPY """ + self.table_name() + " (" + columns + """)
# FROM '""" + filename.replace("\\", "\\\\") + """'
# WITH DELIMITER ','
# CSV HEADER;"""
#             try:
#                 self.execute("BEGIN")
#                 self.execute(statement)
#                 self.execute("COMMIT")
#             except:
#                 self.connection.rollback()
#                 return Engine.insert_data_from_file(self, filename)
#         else:
#             return Engine.insert_data_from_file(self, filename)
        return PostgreSQLEngine.insert_data_from_file(self, filename)

    def format_insert_value(self, value, datatype):
        """Formats a value for an insert statement"""
        return PostgreSQLEngine.format_insert_value(self, value, datatype)

    def check_postgis_availability(self):
        self.cursor.execute("SELECT PostGIS_Version();")
        if self.cursor.fetchone() is not None:
            raster2pgsql = os.system("which raster2pgsql > /dev/null")
            shp2pgsql = os.system("which shp2pgsql > /dev/null")
            if raster2pgsql or shp2pgsql:
                raise Exception("Spatial operations require raster2pgsql and shp2pgsql.\n"
                                + "Please check if these two executables are available in "
                                + "the environment.")
        else:
            raise Exception("PostGIS is not installed.\n")
