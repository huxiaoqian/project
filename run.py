# -*- coding: utf-8 -*-

from optparse import OptionParser

from weibo import create_app

optparser = OptionParser()
optparser.add_option('-p', '--port', dest='port', help='Server Http Port Number', default=9001, type='int')
(options, args) = optparser.parse_args()

if not options.port:
    print 'Usage: python run.py --help'

app = create_app()
app.run(host='0.0.0.0', port=options.port, debug=True)
