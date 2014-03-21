# encoding=utf-8
import os, sys
from sys import stdin, stdout
import pyssdb

c = pyssdb.Client()
print c.get("999480")
