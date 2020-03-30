import psycopg2
import pyodbc
import getpass
import time
import pandas as pd
from collections import defaultdict, namedtuple
import csv
import sys
import os
import subprocess


def timeDec(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


class PostgresDb(object):
    """
        Database connection helper fucntion for PostgreSQL
         :host param: server path
         :db_name param: database name
         :user kwarg: username
         :db_pass kwarg: password
         :quiet kwarg: turns off print statments, useful for multiple writes
        """
    def __init__(self, host, db_name, **kwargs):  # user=None, db_pass=None):
        self.quiet = kwargs.get('quiet', False)
        self.params = {
            'dbname': db_name,
            'user': kwargs.get('user', None),
            'password': kwargs.get('db_pass', None),
            'host': host,
            'port': 5432
        }
        if not kwargs.get('db_pass', None):
            self.db_login()
        self.conn = psycopg2.connect(**self.params)

    def db_login(self):
        if not self.params['user']:
            self.params['user'] = raw_input('User name ({}):'.format(
                self.params['dbname'])).lower()
        self.params['password'] = getpass.getpass('Password ({})'.format(
            self.params['dbname']))

    def dbConnect(self):
        self.conn = psycopg2.connect(**self.params)

    def dbClose(self):
        self.conn.close()

    def query(self, qry):
        output = namedtuple('output', 'data, columns')
        cur = self.conn.cursor()
        qry = qry.replace('%', '%%')
        qry = qry.replace('-pct-', '%')
        try:
            cur.execute(qry)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
            else:
                data = None
                columns = None
                self.conn.commit()
                if not self.quiet:
                    print 'Update sucessfull'
            del cur
            if columns:
                return output(data=data, columns=columns)
            else:
                return output(data=data, columns=None)
        except:
            print 'Query Failed:\n'
            for i in qry.split('\n'):
                print '\t{0}'.format(i)
            self.conn.rollback()
            del cur
            sys.exit()


    def import_table(self, table_name, csv, seperator=','):
        cur = self.conn.cursor()
        with open(csv) as f:
            cur.copy_from(f, table_name, sep=seperator, null='')
        print '{} imported to {}'.format(csv, table_name)
        self.conn.commit()

    def export_table(self, table_name, csv, seperator=','):
        cur = self.conn.cursor()
        with open(csv) as f:
            cur.copy_to(f, table_name, sep=seperator, null='')
        print '{} exported to {}'.format(csv, table_name)


class SqlDb(object):
    """
    Database connection helper fucntion for MS SQL server
     :db_server param: server path
     :db_name param: database name
     :user kwarg: username
     :db_pass kwarg: password
     :quiet kwarg: turns off print statments, useful for multiple writes
    """
    def __init__(self, db_server, db_name, **kwargs):  # user=None, db_pass=None):
        self.quiet = kwargs.get('quiet', False)
        self.params = {
            'DRIVER': 'SQL Server',
            'DATABASE': db_name,
            'UID': kwargs.get('user', None),
            'PWD': kwargs.get('db_pass', None),
            'SERVER': db_server
        }
        if not kwargs.get('db_pass', None):
            self.db_login()
        self.dbConnect()

    def db_login(self):
        """
        if login info has not been passed, get credentials
        :return:
        """
        if not self.params['UID']:
            self.params['UID'] = raw_input('User name ({}):'.format(
                self.params['DATABASE']))
        self.params['PWD'] = getpass.getpass('Password ({})'.format(
            self.params['DATABASE']))
        # will echo in idle, push pass off screen
        print '\n'*1000

    def dbConnect(self):
        self.conn = pyodbc.connect(**self.params)

    def dbClose(self):
        self.conn.close()

    def query(self, qry):
        output = namedtuple('output', 'data, columns')
        cur = self.conn.cursor()
        qry = qry.replace('%', '%%')
        qry = qry.replace('-pct-', '%')
        try:
            cur.execute(qry)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
            else:
                data = None
                columns = None
                self.conn.commit()
                if not self.quiet:
                    print 'Update sucessfull'
            del cur
            if columns:
                return output(data=data, columns=columns)
            else:
                return output(data=data, columns=None)
        except:
            print 'Query Failed:\n'
            for i in qry.split('\n'):
                print '\t{0}'.format(i)
            self.conn.rollback()
            del cur
            sys.exit()


def data_to_dict_data(data, columns):
    dictdata = defaultdict(list)
    for row in data:
        # loop through columns to get index
        for c in range(len(columns)):
            # add row's value by index to dict
            dictdata[columns[c]].append(row[c])
    return dictdata


def query_to_table(db, qry):
    data, col = db.query(qry)  # run query
    dd = data_to_dict_data(data, col)  # convert to dictionary
    df = pd.DataFrame(dd, columns=col)  # convert to pandas dataframe
    return df


def write(out_file, data_to_write, header=[]):
    row_cnt = 0
    with open(out_file, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        # write the header of your output file so the next person reading it knows what the fields are
        if header != []:
            writer.writerow(header)
        # loop through your data and write out
        for row in data_to_write:
            writer.writerow(row)  # this writes the rows to the csv file row needs to be a list
            row_cnt += 1
    return str(row_cnt) + " rows were written to " + str(out_file)


def get_file_loc():
    """TK navigate to file for input - used for error input cases"""

    from Tkinter import Tk
    from tkFileDialog import askopenfilename
    Tk().withdraw()
    filename = askopenfilename()
    print filename
    return filename


def read(in_file='', tries=0):
    row_cnt = 0
    data = []

    try:
        with open(in_file, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                data.append(row)
                row_cnt += 1
        print str(row_cnt) + " rows were read from " + str(in_file)
        return data
    except:
        while tries < 5:
            print 'Your input file was invalid, please select the file you wish to read in. You have ' + str(
                5 - tries) + ' more attempts'
            tries += 1
            return read(get_file_loc(), tries)

def import_dbf_to_pg(import_dbf, pgo, schema='public', gdal_data=r"C:\Program Files (x86)\GDAL\gdal-data"):
    cmd = 'ogr2ogr --config GDAL_DATA "{gdal_data}" -f "PostgreSQL" PG:"host={host} user={user} dbname={dbname} \
    password={password}" {import_dbf} -nln "{tbl_name}" -progress'.format(
        gdal_data=gdal_data,
        host=pgo.params['host'],
        dbname=pgo.params['dbname'],
        user=pgo.params['user'],
        password=pgo.params['password'],
        import_dbf=import_dbf,
        schema=schema,
        tbl_name=os.path.basename(import_dbf)[:-4]
    )
    subprocess.call(cmd, shell=True)


def import_shp_to_pg(import_shp, pgo, schema='public', perc=False, gdal_data=r"C:\Program Files (x86)\GDAL\gdal-data"):
    per = ''
    if perc:
        per = '-lco precision=NO'
    cmd = 'ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs ' \
          'EPSG:{srid} -progress -f "PostgreSQL" PG:"host={host} port=5432 dbname={dbname} ' \
          'user={user} password={password}" "{shp}" -nln {schema}.{tbl_name} {perc}'.format(
        gdal_data=gdal_data,
        srid='2263',
        host=pgo.params['host'],
        dbname=pgo.params['dbname'],
        user=pgo.params['user'],
        password=pgo.params['password'],
        shp=import_shp,
        schema=schema,
        tbl_name=import_shp[:-4],
        perc=per
    )
    subprocess.call(cmd, shell=True)


def add_data_to_pg(pg, dest_table_name, dest_schema=None, seperator='|', tbl='temp_table.csv'):
    """
    assumes tables exist with correct schema
    :return:
    """
    loc_table = dest_schema.lower()+'.'+dest_table_name.lower()
    cur = pg.conn.cursor()
    with open(tbl) as f:
        cur.copy_from(f, loc_table, sep=seperator, null='')
    pg.conn.commit()
    os.remove(os.path.join(os.getcwd(), tbl))  # clean up after yourself in the folder


def import_from_gdb(gdb, feature_name, pgo, schema, gdal_data=r"C:\Program Files (x86)\GDAL\gdal-data"):
    print 'Deleting existing table {s}.{t}'.format(s=schema, t=feature_name)
    pgo.query("DROP TABLE IF EXISTS {s}.{t} CASCADE".format(s=schema, t=feature_name))

    cmd = 'ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs ' \
              'EPSG:{srid} -f "PostgreSQL" PG:"host={host} user={user} dbname={dbname} ' \
              'password={password}" "{gdb}" "{feature}" -nln {sch}.{feature} -progress'.format(
        gdal_data=gdal_data,
        srid=2263,
        host=pgo.params['host'],
        dbname=pgo.params['dbname'],
        user=pgo.params['user'],
        password=pgo.params['password'],
        gdb=gdb,
        feature=feature_name,
        sch=schema)
    os.system(cmd)
    pgo.query("""
            ALTER TABLE {s}.{t}
            RENAME wkb_geometry to geom
        """.format(s=schema, t=feature_name))
    # rename index
    pgo.query("""
            ALTER INDEX IF EXISTS
            {s}.{t}_wkb_geometry_geom_idx
            RENAME to {t}_geom_idx
        """.format(s=schema, t=feature_name))
