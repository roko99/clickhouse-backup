#!/usr/bin/env python3
import getpass
import sys, getopt
import socket
import yaml
import argparse
from clickhouse_driver import Client

def main(argv):
    #conf_file = '/opt/clickhouse-backup/config.yml'
    conf_file = 'D:/config.yml'
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config FILE. (default: '/opt/clickhouse-backup/config.yml')", action="store_true")
    args = parser.parse_args()
    
    user, password, host, port, db, skip_tables = conf_parse(conf_file)
    ch_alive(host, port)

    query = create_query(db, skip_tables)
    print(query)
    #ch_request(password, user, host, port, query)

def ch_alive(host, port):
    sock = socket.socket()
    sock.connect((host,port))
    sock.close()

def ch_request(password, user, host, port, query):
    client = Client(host,
                user=user,
                password=password,
                port=port,
                compression=True)
    result = client.execute(query)
    #print("RESULT: {0}: {1}".format(type(result), result))
    for row in result:
        #print(" ROW: {0}: {1}".format(type(row), row))
        for column in row:
            #print("  COLUMN: {0}: {1}".format(type(column), column))
            print(column)

def help():
    help="""
    clickhouse-backup, usage:

        --config=FILE, -c FILE      Config FILE. (default: "/opt/clickhouse-backup/config.yml")
        --help, -h                  show help
    """
    print (help)

def conf_parse(conf_file):
    with open(conf_file) as file:
        try:
            yamlData = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)
    
    host=yamlData['clickhouse']['host']
    password=yamlData['clickhouse']['password']
    user=yamlData['clickhouse']['username']
    port=yamlData['clickhouse']['port']
    db=yamlData['clickhouse']['db']
    skip_tables=yamlData['clickhouse']['skip']

    return(user, password, host, port, db, skip_tables)

def create_query(db, skip_tables):
    print(skip_tables)
    if skip_tables:
        query = 'SHOW TABLES FROM {} with table'.format(db)
    else:
        query = 'SHOW TABLES FROM {}'.format(db)
    
    return query

main(sys.argv[1:])