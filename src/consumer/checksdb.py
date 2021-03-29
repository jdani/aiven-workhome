#!/usr/bin/env python3

import json
import psycopg2
from loguru import logger


class ChecksDB:
    def __init__(self, uri_file, table):
        # Initialize object
        self.uri_file = uri_file
        self.table = table
        self.uri = self.__get_uri_from_file(self.uri_file)
        self.conn = self.__dbconnect(self.uri)

        # Create table if it does not exist
        if not self.__table_exists(self.table):
            self.__create_table(self.table)


    def __get_uri_from_file(self, uri_file):
        # Read postgresql uri from file
        with open(uri_file, 'r') as f:
            uri = f.read().strip()

        # Minimal error check
        if len(uri.split('\n')) > 1:
            logger.warning('URI in uri_file is longer than 1 line. Probably error connecting....')

        return uri


    def __dbconnect(self, uri):
        # Connect to db
        conn = psycopg2.connect(uri)
        return conn


    def __create_table(self, table):
        # https://www.postgresqltutorial.com/postgresql-python/create-tables/

        # Create table query
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
        """.format(table)

        # Execute query
        create_table_sql = create_table_sql.replace("\n", " ").strip()
        logger.debug("Create table SQL: {}".format(create_table_sql))
        cursor = self.conn.cursor()
        cursor.execute(create_table_sql)
        cursor.close()


    def __table_exists(self, table):
        # Check if table exists
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
        # Generate insert queries

        # Template values part of the query
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

        # String to accumulate values strings
        values = ""
        # For each message
        for json_msg in json_messages:
            # Convert json to dict to replace values in the template
            msg = json.loads(json_msg)
            # Add to values str
            values += values_tmpl.format(**msg).strip()

        # Removing trailing comma
        values = values[:-1]

        # Base insert query
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

        logger.debug("Generated insert query for {} messages".format(len(json_messages)))
        # Return final query
        return insert_query.strip()


    def count_rows(self, table):
        # Get rows in table. Used in tests.
        cursor = self.conn.cursor()
        query = "SELECT COUNT(*) FROM {};".format(table)
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count


    def drop_table(self, table):
        # Drop table. Used in tests.
        cursor = self.conn.cursor()
        count = cursor.execute("DROP TABLE IF EXISTS {};".format(table))
        cursor.close()
        return count


    def insert_json_messages(self, json_messages):
        # If there are messages to be inserted
        if json_messages:
            logger.info("Inserting {} messages".format(len(json_messages)))

            # Get insert query
            insert_query = self.__get_insert_query(json_messages)

            # Execute query
            cursor = self.conn.cursor()
            cursor.execute(insert_query)
            self.conn.commit()
            cursor.close()
        else:
            logger.info("No messages to be inserted")


    def close(self):
        logger.info("Commiting and closing db connection")
        self.conn.commit()
        self.conn.close()
