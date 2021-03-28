#!/usr/bin/env python3

import json
import psycopg2
from loguru import logger


class ChecksDB:
    def __init__(self, uri_file, table):
        self.uri_file = uri_file
        self.table = table
        self.uri = self.__get_uri_from_file(self.uri_file)
        self.conn = self.__dbconnect(self.uri)

        if not self.__table_exists(self.table):
            self.__create_table(self.table)


    def __get_uri_from_file(self, uri_file):
        with open(uri_file, 'r') as f:
            uri = f.read().strip()

        if len(uri.split('\n')) > 1:
            logger.warning('URI in uri_file is longer than 1 line. Probably error connecting....')

        return uri


    def __dbconnect(self, uri):
        conn = psycopg2.connect(uri)
        return conn


    def __create_table(self):
        # https://www.postgresqltutorial.com/postgresql-python/create-tables/
        create_table_sql = """
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            host VARCHAR(256),
            dns_start FLOAT,
            dns_elapsed INT,
            ip VARCHAR(15),
            http_start FLOAT,
            http_elapsed INT,
            http_schema VARCHAR(10),
            http_url_root VARCHAR(256),
            http_path VARCHAR(256),
            http_url VARCHAR(512),
            http_regex VARCHAR(512),
            http_status_code INT,
            http_status_code_reason VARCHAR(512),
            http_retgex_found BOOLEAN
            )
        """
        create_table_sql = create_table_sql.replace("\n", " ").strip()
        logger.debug("Create table SQL: {}".format(create_table_sql))
        cursor = self.conn.cursor()
        cursor.execute(create_table_sql)
        cursor.close()


    def __table_exists(self, table):
        cursor = self.conn.cursor()
        # https://stackoverflow.com/questions/10598002/how-do-i-get-tables-in-postgres-using-psycopg2
        cursor.execute("""
            select exists(select relname from pg_class where relname='{}')
        """.format(table)
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists


    def __get_insert_query(self, json_messages):
        values_tmpl = """
            (
                '{meta[host]}',
                {dns[start]},
                {dns[elapsed]},
                '{dns[ip]}',
                {http[start]},
                {http[elapsed]},
                '{http[schema]}',
                '{http[host]}',
                '{http[path]}',
                '{http[url]}',
                '{http[regex]}',
                {http[status_code]},
                '{http[reason]}',
                {http[regex_found]}
            ),
        """

        values = ""
        for json_msg in json_messages:
            msg = json.loads(json_msg)
            values += values_tmpl.format(**msg).strip()

        # Removing trailing comma
        values = values[:-1]

        insert_query = """
            INSERT INTO {} (
                host,
                dns_start,
                dns_elapsed,
                ip,
                http_start,
                http_elapsed,
                http_schema,
                http_url_root,
                http_path,
                http_url,
                http_regex,
                http_status_code,
                http_status_code_reason,
                http_retgex_found
            ) VALUES
                {}
            ;
        """.format(
            self.table,
            values.strip()
        )

        return insert_query.strip()


    def insert_json_messages(self, json_messages):
        if json_messages:
            insert_query = self.__get_insert_query(json_messages)
            cursor = self.conn.cursor()
            cursor.execute(insert_query)
            self.conn.commit()
            cursor.close()



    def close(self):
        self.conn.commit()
        self.conn.close()
