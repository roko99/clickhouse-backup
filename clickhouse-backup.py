#!/usr/bin/env python3
import getpass
import sys, getopt
import socket
from clickhouse_driver import Client

def main(argv):
    srchost = 'localhost'
    try:
        opts, args = getopt.getopt(argv,"hf:t:",["from=","to="])
    except getopt.GetoptError:
        help()
        sys.exit(2)
    for opt, arg in opts:
        srchost = 'localhost'
        if opt == '-h':
            help()
            sys.exit()
        elif opt in ("-f", "--from"):
            srchost = arg
        elif opt in ("-t", "--to"):
            desthost = arg

    user, pw = credentials()
    chalive(srchost)
    chrequest(pw, user, srchost)

def chalive(srchost):
    sock = socket.socket()
    result = sock.connect_ex((srchost,9000))
    print(result)
    sock.close()

def chrequest(pw, user, srchost):
    client = Client(srchost,
                user=user,
                password=pw,
                compression=True)
    result = client.execute('SELECT 1')
    print("RESULT: {0}: {1}".format(type(result), result))
    for row in result:
        print(" ROW: {0}: {1}".format(type(row), row))
        for column in row:
            print("  COLUMN: {0}: {1}".format(type(column), column))

def credentials():
    return (input('Enter user : '), getpass.getpass('Password : '))

def help():
    help="""
    clickhouse-backup, usage:

        -f - source host, default is localhost
        -t - destination host
    """
    print (help)

main(sys.argv[1:])

