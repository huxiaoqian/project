#!/usr/bin/python


from flup.server.fcgi import WSGIServer
from weibo import create_app

if __name__ == '__main__':
   app = create_app()
   WSGIServer(app, bindAddress='/tmp/weibo-fcgi.sock').run()