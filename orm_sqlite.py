import os
import sqlite3
import progressbar
import subprocess
import zipfile, zlib
import csv
import pandas as pd
import numpy
import glob



class orm_sqlite(object):
    """
    An object for easy interaction with an SQLite database

    Primarily meant for a db holding text articles

    """
    def __init__(self, file_name):
        super(dbORM, self).__init__()
        self.__name__ = file_name


    def connect(self, check_packed=True):
        """Connect to a database.
        Creates connection object (con) and cursor (c)

        """
        self.con = sqlite3.connect(self.__name__)
        self.c = self.con.cursor()

    def close(self):
        """Close database connection"""
        self.c.close()

    def execute(self, command, commit=False):
        """Execute a command

        """
        self.c.execute(command)
        if commit:
            self.commit()

    def commit(self):
        """Commit to database

        """
        self.con.commit()

    def fetch(self, what = "all", size=None):
        """Fetch data from database.
        What can be "ALL", "MANY", or "ONE". Defaults to ALL

        """
        if what.upper() == "ALL":
            return self.c.fetchall()
        elif what.upper() == "MANY":
            if size is not None:
                return self.c.fetchmany(size)
            else:
                return self.c.fetchmany()
        elif what.upper() == "ONE":
            return self.c.fetchone()
        else:
            print("what must be element of 'all', 'many' or 'one'.")

    def drop_table(self, table_name):
        """
        Shorthand for dropping a table.
        Be careful with this.

        """
        cmd="DROP TABLE {}".format(table_name)
        self.execute(cmd)
        self.commit()


    """
    Miscellaneous functions

    """

    def read_text(self, file):
        """
        Read a text file from disk

        """
        file_con = open(file, 'r')
        text = file_con.read()
        return str(text)


    """
    Adding new tables

    """

    def create_table(self, table, col_names, col_types=None, col_constraints=None, other_args=None, overwrite=False):
        """
        Create a table in the database

        table (name) must be provided
        col_names must be provided
        col_types defaults to TXT
        col_constraints defaults to ""
        other_args to add additional arguments

        Example usage:

            db.create_table('Sentiments', col_names = ["File", "Paragraph", "Text", "Sentiment"], col_types = ["TXT", "INT", "TXT", "INT"], other_args = "PRIMARY KEY (File, Paragraph)")

        """
        if overwrite and table in self.list_tables():
            self.drop_table(table)
        ncols = len(col_names)
        if col_types is None:
            col_types = list(numpy.repeat("TXT", ncols))
        if col_constraints is None:
            col_constraints = list(numpy.repeat("", ncols))
        query = [' '.join([cn, cp, cc]) for cn, cp, cc in zip(col_names, col_types, col_constraints)]
        query = "CREATE TABLE {} (".format(table) + ', '.join(query)
        if other_args is not None:
            query = ', '.join([query, other_args])
        query = query + ")"
        self.execute(query)
        self.commit()

    def insert_pandas(self, table, df, overwrite=False):
        """Inserts Pandas DataFrame object to a new or existing table

        Use create_table() first if column flags or so need to be set.

        If overwrite is True, overwrites existing table
        """
        if overwrite:
            try:
                self.drop_table(table)
            except:
                print("No existing table found")
        df.to_sql(table, self.con, if_exists='append', index = False)

    def insert_text_files(self, table, files_path, overwrite=False):
        """Adds all txt files in given directory into a new table
        in the database, using the file name as ID

        table = name of new table where to add the files
        files_path = directory with text files

        Returns nothing

        """
        cols=["File", "Text"]
        p=files_path
        if overwrite:
            try:
                self.drop_table(table)
            except:
                print("No existing table found")
            prim_key="PRIMARY KEY (File)"
            self.create_table(table=table, col_names=cols, other_args=prim_key)
        all_files=os.listdir(p)
        txt_files=[(f,os.path.join(p,f)) for f in all_files if ".TXT" in f.upper()]
        df = pd.DataFrame([(f[0], self.read_text(f[1])) for f in txt_files], columns=cols)
        self.insert_pandas(table, df)


    def insert_csv(self, table, csv_file, overwrite=False):
        """Add CSV file to a table in the database

        Use create_table() first if column flags or so need to be set.
        """
        df = pd.read_csv(csv_file)
        self.insert_pandas(table, df, overwrite=overwrite)


    """
    Selecting data

    """

    def select(self, table, fetch=None, arguments=None):
        """Select query to table

        What defaults to all ('*')

        Fetch is optional, can be either 'all', 'first' or 'many'

        Optional arguments can be passed via `arguments`

        Returns nothing if fetch is None (default)
        """
        query = 'SELECT * FROM {}'.format(table)
        if arguments is not None:
            query = query + " " + arguments
        self.execute(query)
        if fetch is not None:
            res = self.fetch(fetch)
            return res

    def select_query(self, query):
        """Send full select query to database and return results"""
        self.execute(query)
        result = self.fetch()
        return result

    def select_where(self, table, condition):
        """Select * where condition is met"""
        query = 'SELECT * FROM {} WHERE {}'.format(table, condition)
        self.execute(query)
        result = self.fetch()
        return result

    def select_like(self, table, where, like):
        """Select entire table where a specific column contains text"""
        cmd="SELECT * FROM {} WHERE {} LIKE '%{}%'".format(table, where, like)
        self.execute(cmd)
        result = self.fetch()
        return result

    def select_articles(self, like):
        """Get articles where text contains like
        Shorthand for select_like
        """
        result = self.select_like(table='Documents', where='Text', like=like)
        return result

    def get_pandas(self, table, columns="*", arguments=None, chunksize=None):
        """Return a database table as pandas dataframe

        Optional arguments can be passed via `arguments`
        """
        if type(columns) is list: columns=','.join(columns)
        query = "SELECT {} FROM {}".format(columns, table)
        if arguments is not None:
            query = query + " " + arguments
        df = pd.read_sql_query(query, self.con, chunksize=chunksize)
        return df

    """
    Database info / statistics

    """

    def list_tables(self):
        """List tables in database

        Returns list
        """
        query="SELECT name FROM sqlite_master WHERE type='table';"
        self.execute(query)
        output = self.fetch()
        tables = [t[0] for t in output]
        return tables

    def list_columns(self, table):
        """List columns in table

        """
        query='PRAGMA TABLE_INFO({})'.format(table)
        self.execute(query)
        output = self.fetch()
        columns = [tup[1] for tup in output]
        return columns

    def pragma(self, table):
        """Full pragma output for table

        Prints table with column information
            (id, name, type, notnull, default_value, primary_key)

        Returns nothing
        """
        query='PRAGMA TABLE_INFO({})'.format(table)
        self.execute(query)
        output = self.fetch()
        info = [list(tup) for tup in output]
        print("\nColumn Info:\n{:10s}{:25s}{:10s}{:10s}{:12s}{:10s}"\
               .format("ID", "Name", "Type", "NotNull", "DefaultVal", "PrimaryKey"))
        for col in info:
            print_text=tuple(str(t) for t in col)
            print('{:10s}{:25s}{:10s}{:10s}{:12s}{:10s}'.format(*print_text))

    def column_info(self, table):
        """Summary information for columns in table

        Prints table with some pragma information plus actual not null count

        Returns nothing
        """
        query = 'PRAGMA TABLE_INFO({})'.format(table)
        self.execute(query)
        info = self.fetch()
        info = [list(i)[0:3] for i in info] # Only ID, Name, Type
        columns = [i[1] for i in info] # Extract columns
        for i, col in enumerate(columns):
            count = self.count_notnull(col, table)
            info[i].append(count)
        print("\nColumn Info:\n{:10s}{:25s}{:10s}{:10s}"\
               .format("ID", "Name", "Type", "NotNull"))
        for col in info:
            print_text=tuple(str(t) for t in col)
            print('{:10s}{:25s}{:10s}{:10s}'.format(*print_text))

    def count(self, column, table):
        """Count number of rows

        returns int

        """
        query = "SELECT COUNT({}) FROM {}".format(column, table)
        self.execute(query)
        count = self.fetch()
        return int(count[0][0])

    def count_where(self, column, table, condition):
        """count rows where condition is met"""
        query = "SELECT COUNT({}) FROM {} WHERE {}".format(column, table, condition)
        self.execute(query)
        count = self.fetch()
        return int(count[0][0])

    def count_distinct(self, column, table):
        """Count distinct entries

        Returns int
        """
        query = "SELECT COUNT(DISTINCT {}) FROM {}".format(column, table)
        self.execute(query)
        count = self.fetch()
        return int(count[0][0])

    def count_notnull(self, what, where):
        """Count non-null entries in column

        Returns int
        """
        query='SELECT COUNT({0}) FROM {1} WHERE {0} IS NOT NULL'.format(what, where)
        self.execute(query)
        count = self.fetch()
        return int(count[0][0])

    def count_like(self, what, where, like):
        """Count number of rows containing text (`like`)

        Returns int
        """
        cmd="SELECT COUNT({}) FROM {} WHERE {} LIKE '%{}%'".format(what, where, what, like)
        self.execute(cmd)
        count =self.fetch()
        return count[0][0]

    def count_articles(self, like):
        """Count articles matching text (`like`)

        Shorthand function for count_like() with what='Text' and
        where='documents'

        Returns int
        """
        result = self.count_like(like=like, what="Text", where="Documents")
        return result
