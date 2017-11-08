#!/usr/bin/env python
#credit: python docs
import SimpleHTTPServer
import SocketServer
import argparse

parser = argparse.ArgumentParser("Super simple HTML webserver for debugging purposes")
add = parser.add_argument
add("-p", "--port", type=int, default=8888, help="Listening port")
args = parser.parse_args()

Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
httpd = SocketServer.TCPServer(("", args.port), Handler)

print "serving at port", args.port
httpd.serve_forever()
