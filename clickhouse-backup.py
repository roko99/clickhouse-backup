#!/usr/bin/env python3
import getpass
import sys, getopt
import socket
import yaml
import argparse
from clickhouse_driver import Client

def main(argv):
    #confFile = '/opt/clickhouse-backup/config.yml'
    confFile = 'D:/config.yml'
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config FILE. (default: '/opt/clickhouse-backup/config.yml')", action="store_true")
    args = parser.parse_args()
    user, password, host, port, db = confparse(confFile)
    chalive(host, port)

    queryForm(db, skipTables)
    #chrequest(password, user, host, port, query)

def chalive(host, port):
    sock = socket.socket()
    sock.connect((host,port))
    sock.close()

def chrequest(password, user, host, port, query):
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

def confparse(confFile):
    with open(confFile) as file:
        try:
            yamlData = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)
    
    host=yamlData['clickhouse']['host']
    password=yamlData['clickhouse']['password']
    user=yamlData['clickhouse']['username']
    port=yamlData['clickhouse']['port']
    db=yamlData['clickhouse']['db']
    skipTables=yamlData['clickhouse']['skip']

    return(user, password, host, port, db)

def queryForm(db, skipTables):
    print(skipTables)
    #if skipTables 
    query = 'SHOW TABLES FROM {}'.format(db)

main(sys.argv[1:])