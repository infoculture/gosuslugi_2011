#!/usr/bin/env python
# -*- coding: utf8 -*-
import sys
from pymongo import Connection
import socket
from urllib import urlopen
import csv
from StringIO import StringIO
import simplejson as json
from lxml import etree
from urlparse import urljoin
from lxml.html import fromstring
from BeautifulSoup import UnicodeDammit
import re

DBNAME = 'gs'
DOM_COLL = "domains"
MX_COLL = 'mxservers'
EM_COLL = 'emails'
SERV_COLL = 'services'


class GosuslugiReportBuilder:
    def __init__(self, filename='urllist.txt'):
        self.filename = filename
        self.conn = Connection()
        self.db = self.conn[DBNAME]
        self.coll = self.db['orgs']
        self.dcoll = self.db[DOM_COLL]
        self.mxcoll = self.db[MX_COLL]
        self.emcoll = self.db[EM_COLL]
        self.servicecoll = self.db[SERV_COLL]
        pass

    def full_report(self):
        all = self.coll.find({'root' : True})
        for o in all:
            l = [str(self.coll.find({'rootkey' : o['key']}).count()), o['name']]
            print (u'|'.join(l)).encode('utf8')
                                         

        
if __name__ == "__main__":
    socket.setdefaulttimeout(15)
    p = GosuslugiReportBuilder()
    p.full_report()
