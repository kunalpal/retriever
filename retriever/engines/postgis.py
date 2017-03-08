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
                            + ["CREATE EXTENSION IF NOT EXISTS {}".format(extension)
                               for extension in ["postgis", "postgis_sfcgal", "postgis_topology"]])

    def create_table(self):
        """PostgreSQL needs to commit operations individually."""
        return PostgreSQLEngine.create_table(self)

    def insert_data_from_file(self, filename, table_name="raster"):
        if hasattr(self.script, "spatial"):
            if self.script.spatial is "raster":
                schema_name = self.script.shortname + "." + table_name
                sql_save_path = self.format_filename(schema_name + ".sql")
                print("=> Processing {}".format(schema_name))
                print("Preaparing intermediate SQL file.")
                command = "raster2pgsql -c {} {} > {}".format(filename, schema_name, sql_save_path)
                if not os.system(command):
                    self.get_cursor()
                    with open(sql_save_path) as sql_content:
                        try:
                            self.cursor.execute(sql_content.read())
                        except Exception as e:
                            self.connection.rollback()
                            raise e
                        finally:
                            sql_content.close()
                            os.system("rm -rf {}".format(sql_save_path))
                else:
                    raise Exception("Unable to parse file using raster2pgsql")
            else:
                raise Exception("Currently only raster types are supported.")
        else:
            return PostgreSQLEngine.insert_data_from_file(self, filename)

    def format_insert_value(self, value, datatype):
        """Formats a value for an insert statement"""
        return PostgreSQLEngine.format_insert_value(self, value, datatype)

    @staticmethod
    def check_postgis_availability():
        # self.get_cursor()
        # self.cursor.execute("SELECT PostGIS_Version();")
        # if self.cursor.fetchone() is not None:
        raster2pgsql = os.system("which raster2pgsql > /dev/null")
        shp2pgsql = os.system("which shp2pgsql > /dev/null")
        if raster2pgsql or shp2pgsql:
            raise Exception("Spatial operations require raster2pgsql and shp2pgsql.\n"
                            + "Please check if these two executables are available in "
                            + "the environment.")
        # else:
            # raise Exception("PostGIS is not installed.\n")
