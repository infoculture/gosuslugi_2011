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
from lxml.html import fromstring, tostring
from BeautifulSoup import UnicodeDammit


DBNAME = 'gs'

def decode_html(html_string):
        converted = UnicodeDammit(html_string, isHTML=True)
        if not converted.unicode:
                raise UnicodeDecodeError("Failed to detect encoding, tried [%s]", ', '.join(converted.triedEncodings))
        return converted.unicode

class UniParser:
    def __init__(self):
        pass
    
    def get_page(self, url):
        f = urlopen(url)
        data = f.read()
        rurl = f.geturl()
        f.close()
        data = decode_html(data)
        root = fromstring(data)
        return root, rurl         
    
    def process_row(self,row):
        cells = []
        for cell in row.xpath('./td'):
            inner_tables = cell.xpath('./table')
            if len(inner_tables) < 1:
                cells.append(cell.text_content().replace('\r',' ').replace('\n', ' ').encode('utf8'))
            else:
                cells.append([self.process_table(t) for t in inner_tables])
        return cells

    def process_table(self, table):
        return [self.process_row(row) for row in table.xpath('./tr')]


    def parseList(self, url, xpath, absolutize=True):
        root, rurl = self.get_page(url)
        results = []
        links = root.xpath(xpath)
        for l in links:
            href = l.attrib['href']
            text = l.text
            item = [text.encode('utf8'), urljoin(rurl, href.encode('utf8')) if absolutize else href.encode('utf8')]
            results.append(item)
        return results    

    def parseOptionsList(self, url, xpath, absolutize=True):
        root, rurl = self.get_page(url)
        results = []
        links = root.xpath(xpath)
        for l in links:            
            href = l.attrib['value'] if l.attrib.has_key('value') else None
            if href:
                href = urljoin(rurl, href.encode('utf8')) if absolutize else href.encode('utf8')
            text = l.text
            if text:
                text = text.encode('utf8')
            item = [text, href]
            results.append(item)
        return results             

    def parseTable(self, url, page, xpath):
        results = []
        objects = page.xpath(xpath)
        if len(objects) > 0:
            results = self.process_table(objects[0])
        return results

    def getBlock(self, url, page, xpath):
        results = []
        objects = page.xpath(xpath)        
        if len(objects) > 0:
            return etree.tostring(objects[0], pretty_print=True)
        return None

    def getTextList(self, url, xpath, clean_empty=True, stop_on=None):
        root, rurl = self.get_page(url)
        results = []
        objects = root.xpath(xpath)        
        if len(objects) > 0:
            for o in objects:
                if stop_on is not None and stop_on == o.tag:
#                    print stop_on, o.tag
                    break    
                for item in o.itertext():
                    t = item.strip()
                    if len(t) > 0:
                        results.append(t)
        return results




class GosuslugiParser:
    def __init__(self, filename='urllist.txt'):
        self.filename = filename
        self.conn = Connection()
        self.db = self.conn[DBNAME]
        self.coll = self.db['orgs']
        self.pagecoll = self.db['pages']
        pass

    def get_page(self, url):
        f = urlopen(url)
        data = f.read()
        rurl = f.geturl()
        f.close()
        data = decode_html(data)
        root = fromstring(data)
        return root, rurl         
    
    def process_row(self,row):
        cells = []
        for cell in row.xpath('./td'):
            inner_tables = cell.xpath('./table')
            if len(inner_tables) < 1:
                cells.append(cell.text_content().replace('\r',' ').replace('\n', ' ').encode('utf8'))
            else:
                cells.append([self.process_table(t) for t in inner_tables])
        return cells

    def process_table(self, table):
        return [self.process_row(row) for row in table.xpath('./tr')]


    def parseList(self, url, page, xpath, absolutize=True):
        results = []
        links = page.xpath(xpath)
        for l in links:
            href = l.attrib['href']
            text = l.text
            item = [text.encode('utf8'), urljoin(url, href.encode('utf8')) if absolutize else href.encode('utf8')]
            results.append(item)
        return results    

    def parseTable(self, url, page, xpath):
        results = []
        objects = page.xpath(xpath)
        if len(objects) > 0:
            results = self.process_table(objects[0])
        return results


    def update_all(self):  
        all = self.coll.find({'suborgs.exists' : True}).sort('_id', 1)
        for obj in all:
#            if obj['name'].find(u'ПФР') > -1: continue
#            if obj['name'].find(u'енсионный') > -1: continue
#            if obj['name'].find(u'ГИБДД') > -1: continue
            for org in obj['suborgs']['items']:
                url = org['url']
                name = org['name']
                print url, 'scheduled'
                key = self.getkey(url)
                try:
                    o = self.coll.find_one({'key' : key.encode('utf8', 'replace')})
                except UnicodeDecodeError:
                    print 'Error:', 'UnicodeDecodeError'
                    continue
                if o is not None: continue
                item = self.parse_url(url, name)
                if item is None: 
                    print url, 'error downloading'
                    continue
                self.coll.save(item)
                print url, 'processed'


    def parse_all(self):
        f = open(self.filename, 'r')
        cr = csv.reader(f, delimiter='\t')
        for row in cr:    
            name, url = row
            print url, 'scheduled'
            key = self.getkey(url)
            o = self.coll.find_one({'key' : key})
            if o is not None: continue
            item = self.parse_url(url, name)            
            self.coll.save(item)
            print url, 'processed'

    def __identify_tab(self, text):
        if text == u'Места обращения':
            tkey = 'places'
        elif text == u'Подведомственные организации':
            tkey = 'suborgs'
        elif text == u'Государственные услуги':
            tkey = 'services'
        elif text == u'Контактные лица':
            tkey = 'contacts'
        else:
            tkey = 'unknown'
            print text
        return tkey


    def setkeys(self):
        for o in self.coll.find():
            o['key'] = self.getkey(o['url'])
            self.coll.save(o)
               
         

    def getkey(self, url):
        key = ''
        params = url.split('?')[1]
        kv = params.split('&')
        for i in kv:
            parts = i.split('=')
            if len(parts) > 0 and parts[0] == 'ssid_4': key = parts[1]
        return key


    def parse_url(self, url, name, tkey=None):  
        item = {'url' : url, 'name': name}
        item['key'] = self.getkey(url)
        if tkey is not None:
            print '-', url, tkey
        try:
            page, rurl = self.get_page(url)
        except KeyboardInterrupt:
            sys.exit(0)
        except:
            return None
        if tkey is None:
            tab = self.parseTable(rurl, page, "//table[@class='person_tbl']")       
            item['profile'] = tab
        menu_sel = page.xpath('//div[@class="menu1 sel"]')
        if tkey is None:
            if len(menu_sel) > 0:
                text = menu_sel[0].text.strip()
                tabkey = self.__identify_tab(text)
            else:
                tabkey = 'unknown'
        else:
            tabkey = tkey
        item[tabkey] = {'exists' : True, 'items' : []}
        if tkey is None:
            menu_all = page.xpath('//div[@class="menu1"]')
            print menu_all
            for m in menu_all:
                ah = m.xpath('a')[0]
                url = 'http://www.gosuslugi.ru' + ah.attrib['href']
                tname = ah.text.strip()
                ident = self.__identify_tab(tname)
                print 'Tab type', ident
                val = self.parse_url(url, name, ident)
                if val:
                    item.update(val)
        if tabkey == 'suborgs':
            t_list = self.parseList(rurl, page, "//ul[@class='state_structure_childs2']/li/a")                      
            for t in t_list:
                item[tabkey]['items'].append({'name' : t[0], 'url' : t[1]})
        elif tabkey == 'contacts':
            tab = self.parseTable(rurl, page, "//table[@class='table_admin']")      
            item[tabkey]['items'] = tab
        elif tabkey == 'places':
            tab = self.parseTable(rurl, page, "//table[@class='table_admin']")      
            item[tabkey]['items'] = tab
        elif tabkey == 'services':
            t_list = self.parseList(rurl, page, "//div[@class='div5 div12 ']/a")                        
            for t in t_list:
                item[tabkey]['items'].append({'name' : t[0], 'url' : t[1]})
            t_list = self.parseList(rurl, page, "//div[@class='div5 div12 list74']/a")                      
            for t in t_list:
                item[tabkey]['items'].append({'name' : t[0], 'url' : t[1]})
        return item
        
        
    def analyze_info(self):    
        name = u'Электронная почта:'
        for item in self.coll.find():
            for o in item['profile']:
                if o[0] == name:
                    print o[1].encode('utf8')

    def analyze_contacts(self):    
        for item in self.coll.find({'contacts.exists' : True}):
            for o in item['contacts']['items']:
                if len(o) > 3:
                    print o[3].encode('utf8')

    def store_pages(self):
        all = []
        for org in self.coll.find():
            all.append(org)
        for org in all:
            url = org['url'] 
            try:
                o = self.pagecoll.find_one({'url' : url})
                if o is not None: continue
                page, rurl = self.get_page(url)
                elem = page.xpath("//div[@class='content']")[0]
                self.pagecoll.save({'url' : url, 'rurl' : rurl, 'page' : tostring(elem)})
                print 'Processed', url
            except KeyboardInterrupt:
                sys.exit(0)
            except:
                print 'Error getting', url
                continue

        
if __name__ == "__main__":
    socket.setdefaulttimeout(5)
    p = GosuslugiParser()
#    p.parse_all()
#    p.update_all()
    p.store_pages()
#    p.analyze_info()
#    p.analyze_contacts()
#    p.update_all()
