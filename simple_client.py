

import socket
import pickle
import sys

MSG_JOIN = "JOIN"
MSG_HELLO = "HELLO"
SOCKET_TIMEOUT_IN_MILLIS = 1500
BUF_SIZE_IN_BYTES = 1024

if (len(sys.argv) != 3):
    print ("Expecting 2 arguments and received '%i' argument(s). \
Example usage: 'python lab1.py hostName portNum'" % (len(sys.argv) - 1))
    exit(1);

print ("\nExecuting script name = '%s'...\n" % sys.argv[0])

hostname = sys.argv[1]

try:
    port = int(sys.argv[2])
except ValueError:
    print ("Failed to convert port argument to an integer.\n")
    raise

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcdsocket:
    try:
        gcdsocket.connect((hostname, port))
        print ("%s ('%s' %i)\n" % (MSG_JOIN, hostname, port))
        gcdsocket.sendall(pickle.dumps(MSG_JOIN))
        gcdresp = pickle.loads(gcdsocket.recv(BUF_SIZE_IN_BYTES))
        for dict in gcdresp:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mysocket:
                    print ("%s to %s" % (MSG_HELLO, dict))
                    mysocket.settimeout(SOCKET_TIMEOUT_IN_MILLIS / 1000)
                    mysocket.connect((dict["host"], int(dict["port"])))
                    mysocket.sendall(pickle.dumps(MSG_HELLO))
                    grpmemresp = pickle.loads(mysocket.recv(BUF_SIZE_IN_BYTES))
                    print(grpmemresp)
            except OSError as err:
                print("failed to connect: {0}".format(err))
            except:
                print("Encountered unexpected error:", sys.exc_info()[0])
                raise
            finally:
                mysocket.close()
    except OSError as err:
        print("failed to connect: {0}".format(err))
    except:
        print("Encountered unexpected error:", sys.exc_info()[0])
    finally:
        gcdsocket.close()
exit(1)
