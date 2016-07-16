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

def mark_word(w):
    s = u""
    for ch in w:
        if ch in chrange(u'a', u'z') or ch in chrange(u'A', u'Z') : 
            s += u'<b style="color:red">' + ch +'</b>'
        elif ch in chrange(u'а', u'я') or ch in chrange(u'А', u'Я'): 
            s += u'<b style="color:green">' + ch +'</b>'
        else: 
            s += ch
    return s

def chrange(char1, char2): 
    return [unichr(i) for i in range(ord(char1), ord(char2)+1)] 

def is_mixed_word(w):
    is_r = False
    is_e = False
    for ch in w:
        if ch in chrange(u'a', u'z'): is_e = True
        if ch in chrange(u'A', u'Z'): is_e = True
        if ch in chrange(u'а', u'я'): is_r = True
        if ch in chrange(u'А', u'Я'): is_r = True
    if is_r and is_e: return True
    else: return False


def decode_html(html_string):
        converted = UnicodeDammit(html_string, isHTML=True)
        if not converted.unicode:
                raise UnicodeDecodeError("Failed to detect encoding, tried [%s]", ', '.join(converted.triedEncodings))
        return converted.unicode

def validateEmail(email):
    if re.match("^([\w\!\#$\%\&\'\*\+\-\/\=\?\^\`{\|\}\~]+\.)*[\w\!\#$\%\&\'\*\+\-\/\=\?\^\`{\|\}\~]+@((((([a-z0-9]{1}[a-z0-9\-]{0,62}[a-z0-9]{1})|[a-z])\.)+[a-z]{2,6})|(\d{1,3}\.){3}\d{1,3}(\:\d{1,5})?)$", email) != None:    
    #if re.match("^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$",email) != None:
        return True
    return False

def dns_query(domain, qtype="A"):
    url = 'http://apibeta.skyur.ru/dns/query/?query=%s&qtype=%s' %(domain, qtype)
    print url
    f = urlopen(url)
    data = f.read()
    f.close()
    print data
    return json.loads(data)


class GosuslugiParser:
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

    def getkey(self, url):
        """Returns organization key ssid_4"""
        key = ''
        params = url.split('?')[1]
        kv = params.split('&')
        for i in kv:
            parts = i.split('=')
            if len(parts) > 0 and parts[0] == 'ssid_4': key = parts[1]
        return key

    def get_domain(self, email):
        """Extracts domain name from email"""
        name, domain = email.rsplit('@', 1)
        return domain.lower()
    
    def process_mx(self):
        """Processes MX records for domains."""
        domains = self.dcoll.find()
        self.mxcoll.remove()
        for d in domains:
            if d['has_mx'] == True:
                for m in d['mx']:
                    mxname = m['name']
                    one = self.mxcoll.find_one({'domain' : mxname})
                    if one is None:
                        parts = mxname.rsplit('.', 2)
                        one = {'domain' : mxname, 'l2_dom' : '.'.join(parts[-2:])}                        
                        one['num_domains'] = 0
                        one['domains'] = []
                    one['num_domains'] += 1
                    one['domains'].append(d['domain'])
                    self.mxcoll.save(one)

    def dom_cleanup(self):
        domains = self.dcoll.find()
        for d in domains:
            if d['has_mx'] == True:
                items = []
                for m in d['mx']:
                    m['name'] = m['name'].lower().rstrip('.')
                    parts = m['name'].rsplit('.', 2)                    
                    items.append({'priority' : m['priority'], 'name' : m['name'], 'l2_dom' : '.'.join(parts[-2:])})
                d['mx'] = items
            self.dcoll.save(d)

    def process_services(self):
        self.servicecoll.remove()
        for org in self.coll.find():
            if org.has_key('services') and org['services']['exists']:
                for service in org['services']['items']:
                    item = self.servicecoll.find_one({'url' : service['url']})
                    if item is None:                    
                        item = {'url' : service['url'], 'name' : service['name'], 'num_orgs' : 1, 'orgs' : [org['key'],]}                    
                        parts = service['url'].rsplit('?')[1].split('&')
                        for s in parts:
                            k, v = s.split('=')
                            item[k] = v                            
                    else:
                        if org['key'] not in item['orgs']:
                            item['num_orgs'] +=1
                            item['orgs'].append(org['key']) 
                    self.servicecoll.save(item)
                    
    def update_orgs(self):
        """Adds keys to the records"""
        i = 0        
        for org in self.coll.find():
            i += 1
            if i % 100 == 0: print i
            if org.has_key('suborgs') and org['suborgs']['exists'] == True:
                items = []
                for o in org['suborgs']['items']:
                    key = self.getkey(o['url'])
                    o['key'] = key            
                    items.append(o)                                        
                org['suborgs']['items'] = items
                self.coll.save(org)
                
    def find_org_parents(self):
        i = 0        
        all = []
        for org in self.coll.find():
            all.append(org)
        for org in all:
            i += 1
            if i % 100 == 0: print i
            if org.has_key('suborgs') and org['suborgs']['exists'] == True:
                for o in org['suborgs']['items']:
                    key = o['key']
                    child = self.coll.find_one({'key' : key})
                    if child.has_key('parent'):
                        if child.has_key('all_parents'): 
                            if org['key'] not in child['all_parents']:                                
                                child['all_parents'].append(org['key'])
                        else:
                            child['all_parents'] = [child['parent'], ]
                            if org['key'] not in child['all_parents']:                                
                                child['all_parents'].append(org['key'])
#                        print 'Child', key, 'already has parent', child['parent']
                    else:
                        child['parent'] = org['key']
                        child['all_parents'] = [org['key'], ]
                    self.coll.save(child)

 
    def cleanup_parents(self):
        all = []
        for org in self.coll.find():
            all.append(org)
        i = 0
        for org in all:
            i += 1
            if i % 100 == 0: print i
            if org.has_key('all_parents'):
                del org['all_parents']# = {'num' : len(org['all_parents']), 'list' : org['all_parents']}
                self.coll.save(org)

    def parse_profile(self, profile):
        results = {}
        for k,v in profile:
            if k == u'Руководитель организации:':
                results['chief'] = v
            elif k == u'Веб-сайт:':
                results['website'] = v
            elif k == u'Электронная почта:':
                results['email'] = v
            elif k == u'Автоинформатор:':
                results['informer'] = v
            elif k == u'Режим работы:':
                results['workschedule'] = v
            elif k == u'Время работы экспедиции:':
                results['expschedule'] = v
            else:
                print k
                sys.exit(0)
        return results

    def dump_orgs(self):
        keys = ['key', 'name','url', 'has_profile', 'has_email', 'has_informer', 'has_chief', 'has_website', 'has_workschedule', 'has_expschedule', 'chief', 'informer', 'email', 'website', 'workschedule', 'expschedule', 'has_parent', 'parent', 'two_parents', 'has_services', 'num_services', 'has_contacts', 'num_contacts', 'has_suborgs', 'num_suborgs', 'has_places', 'num_places']
        rec = []
        for k in keys:
            rec.append(k)
        print (u'|'.join(map(unicode, rec))).encode('utf8')        
        for org in self.coll.find():
            item = {}
            profile = self.parse_profile(org['profile'])
            item['key'] = org['key']
            item['name'] = org['name']
            item['url'] = org['url']              
            item['has_profile'] = str((len(org['profile']) > 0))
            item['has_email'] = profile.has_key('email')
            item['has_informer'] = profile.has_key('informer') 
            item['has_chief'] = profile.has_key('chief') 
            item['has_website'] = profile.has_key('website') 
            item['has_workschedule'] = profile.has_key('workschedule') 
            item['has_expschedule'] = profile.has_key('expschedule') 
            for k in ['chief', 'informer', 'email', 'website', 'workschedule', 'expschedule']:
                item[k] = profile[k] if profile.has_key(k) else ''            
            item['has_parent'] = str(org.has_key('parent'))
            item['parent'] = org['parent'] if org.has_key('parent') else ''
            item['two_parents'] = str((len(org['all_parents']) > 1)) if org.has_key('all_parents') else False
            item['has_services'] =  (org.has_key('services') and org['services']['exists'])
            item['num_services'] = len(org['services']['items']) if (org.has_key('services') and org['services']['exists']) else 0
            item['has_contacts'] =  (org.has_key('contacts') and org['contacts']['exists'])
            item['num_contacts'] = len(org['contacts']['items']) if (org.has_key('contacts') and org['contacts']['exists']) else 0
            item['has_suborgs'] =  (org.has_key('suborgs') and org['suborgs']['exists'])
            item['num_suborgs'] = len(org['suborgs']['items']) if (org.has_key('suborgs') and org['suborgs']['exists']) else 0
            item['has_places'] =  (org.has_key('places') and org['places']['exists'])
            item['num_places'] = len(org['places']['items']) if (org.has_key('places') and org['places']['exists']) else 0
            rec = []
            for k in keys:
                rec.append(item[k])
            print (u'|'.join(map(unicode, rec))).encode('utf8')
        

    def remove_double_parents(self):
        csvr = csv.DictReader(open('aps.txt', 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
        keys = {}
        for r in csvr:
            rk = [r['left_key'], r['right_key']]
            rk.sort()
            keys['-'.join(rk)] = r['chosen_key']
        i = 0
        all = []
        for org in self.coll.find():
            i += 1
            if i % 500 == 0: print i
            all.append(org)
        i = 0
        for org in all:
            i += 1
            if i % 100 == 0: print i
            if org.has_key('all_parents') and len(org['all_parents']) > 1:
                if len(org['all_parents']) == 2:                
                    ap = org['all_parents']
                    ap.sort()
                    k = '-'.join(ap)
                    if not keys.has_key(k): continue
                    chosen = keys[k]
                    org['all_parents'] = [chosen, ]
                    org['parent'] = chosen
                    self.coll.save(org)
        


    def find_double_parents(self):        
        aps = []
        all = []
        for org in self.coll.find():
            all.append(org)
        i = 0
        for org in all:
            i += 1
#            if i % 100 == 0: print i
            if org.has_key('all_parents') and len(org['all_parents']) > 1:
                if len(org['all_parents']) == 2:                
                    ap = org['all_parents']
                    left = self.coll.find_one({'key' : ap[0]})
                    right = self.coll.find_one({'key' : ap[1]})
                    if right.has_key('parent') and left['key'] == right['parent']:
                        org['parent'] = right['key']
#                        self.coll.save(org)
                        print org['key'], 'found'
                    elif left.has_key('parent') and  right['key'] == left['parent']:
                        org['parent'] = left['key']
#                        print org['key'], 'found'
                        self.coll.save(org)
                    else:
                        ap.sort()
                        s = '-'.join(ap)
                        if s not in aps:
                            aps.append(s)
                            rec = [left['key'], left['name'], right['key'], right['name']]
                            print (u'\t'.join(rec)).encode('utf8')
                else:
                    org['bad'] = True
#                    print org['key'], 'BAD'
                    self.coll.save(org)
        for s in aps:
            print s

    def __map_org_level(self, org, level=1):
        if not org.has_key('level'):
            org['level'] = level
            self.coll.save(org)
        for ch in org['childs']['list']:
            child = self.coll.find_one({'key' : ch})
            self.__map_org_level(child, level+1)

    def map_levels(self):
        all = []
        i = 0
        for org in self.coll.find():
            i += 1
            if i % 100 == 0:
                print i
            all.append(org)
        i = 0
        for org in all:
            i += 1
            if i % 100 == 0: print i
#            if org.has_key('childs'): continue
            org['childs'] = {'num' : 0, 'list' : []}
            children = self.coll.find({'parent' : org['key']})
            for ch in children:
                org['childs']['num'] += 1
                org['childs']['list'].append(ch['key'])
            self.coll.save(org)
        i = 0
        
        for org in self.coll.find({'parent' : {'$exists' : False}}):
            self.__map_org_level(org, level=1)
            
        

    def process_emails(self):    
        name = u'Электронная почта:'
        domains = []
        self.emcoll.remove()
        for org in self.coll.find():
            for o in org['profile']:
                if o[0] == name:
                    email = o[1].encode('utf8').strip().lower()
                    em = self.emcoll.find_one({'email' : email})
                    if em is not None: continue
                    print email
                    item = {'email' : email}
                    if validateEmail(email):     
                        item['valid'] = True                                       
                        d = self.get_domain(email)
                        item['domain'] = d
                        dom = self.dcoll.find_one({'domain' : d})
                        if dom is not None:
                            item['parsed'] = True
                            del dom['_id']
                            item.update(dom)
                        else:
                            item['parsed'] = False
                    else:
                        item['valid'] = False
                    print item
                    self.emcoll.save(item)
            if org.has_key('contacts') and org['contacts']['exists']:
                for o in org['contacts']['items']:
                    if len(o) > 3:
                        email = o[3].encode('utf8').strip().lower()
                        em = self.emcoll.find_one({'email' : email})
                        if em is not None: continue
                        print email
                        item = {'email' : email}                        
                        if validateEmail(email):     
                            item['valid'] = True                                       
                            d = self.get_domain(email)
                            item['domain'] = d
                            dom = self.dcoll.find_one({'domain' : d})
                            if dom is not None:
                                item['parsed'] = True
                                del dom['_id']
                                item.update(dom)
                            else:
                                item['parsed'] = False
                        else:
                            item['valid'] = False
                        print item
                        self.emcoll.save(item)
    
    def analyze_domains(self):    
        name = u'Электронная почта:'
        domains = []
        for item in self.coll.find():
            for o in item['profile']:
                if o[0] == name:
                    email = o[1].encode('utf8').strip().lower()
                    if validateEmail(email):                        
                        d = self.get_domain(email)
                        if d not in domains:
                            domains.append(d)                                
        for d in domains:
            dom = self.dcoll.find_one({'domain' : d})
            if dom is not None: continue
            item = {'domain' : d}
            try:
                res = dns_query(d, 'A')
            except:
                continue
            if len(res['response']) == 0:
                item['has_a'] = False
            else:
                item['has_a'] = True
                item['a'] = []
                for o in res['response']:
                    item['a'].append({'name' : o})
            res = dns_query(d, 'MX')
            if len(res['response']) == 0:
                item['has_mx'] = False
            else:
                item['has_mx'] = True
                item['mx'] = []
                for o in res['response']:
                    priority, name = o.split()
                    item['mx'].append({'name' : name.rstrip('.'), 'priority' : priority})
            print item
            self.dcoll.save(item)
            
    
    def analyze_info(self):    
        name = u'Электронная почта:'
        for item in self.coll.find():
            for o in item['profile']:
                if o[0] == name:
                    email = o[1].encode('utf8').strip().lower()
                    if not validateEmail(email):
                        print email.strip() 

    def analyze_contacts(self):    
        for item in self.coll.find({'contacts.exists' : True}):
            for o in item['contacts']['items']:
                if len(o) > 3:
                    print o[3].encode('utf8')

    def find_latin(self):
        rf = open('latin.html', 'w')
        rf.write("""
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="ru" lang="ru"> 
<head> 
<meta http-equiv="Content-Type" content="text/html; charset=utf8" /> 
</head> 
<body> 
""")
        rf.write('<table border="1">\n')
        for item in self.coll.find():
            words = item['name'].split()
            nw = []
            got_marked = False
            for w in words:
                if is_mixed_word(w):
                    mw = mark_word(w)
                    nw.append(mw)               
                    got_marked = True
                else:
                    nw.append(w)
            if got_marked:
                rf.write((u"<tr><td><a href='%s'>%s</a></td><td>%s</td><td>%s</td></tr>" %(item['url'], item['key'], u' '.join(nw), "<b style='color:red'>!</b>" if got_marked else ".")).encode('utf8'))
                rf.write('\n')
        rf.write("</table></body></html>\n")
        rf.close()

    def __map_childs(self, childs, level, rootkey):
        for key in childs:
            o = self.coll.find_one({'key' : key})
            if o is None: continue
            if o.has_key('root') and o['root']: continue
            o['rootkey'] = rootkey
            o['root'] = False
            o['level'] = level
            self.coll.save(o)
            if o.has_key('childs') and o['childs']['num'] > 0:
                l2 = level + 1
                self.__map_childs(o['childs']['list'], l2, rootkey)

    def map_root(self):
        f = open(self.filename, 'r')
        cr = csv.reader(f, delimiter='\t')
        for row in cr:    
            name, url = row
            key = self.getkey(url)
            o = self.coll.find_one({'key' : key})
            if o is None: continue
            o['root'] = True
            o['rootkey'] = o['key']
            o['level'] = 1
            self.coll.save(o)
            print url, 'processed'
            print '- child map start'
            if o.has_key('childs') and o['childs']['num'] > 0:
                l2 = 2
                self.__map_childs(o['childs']['list'], l2, o['key'])
            print '- child map end'

    def map_root_extend(self):
        for o in self.coll.find({'root' : True, 'parent': {'$exists' : True}}):
            o['root'] = True
            o['rootkey'] = o['key']
            o['level'] = 1
            self.coll.save(o)
            print o['url'], 'processed'
            print '- child map start'
            if o.has_key('childs') and o['childs']['num'] > 0:
                l2 = 2
                self.__map_childs(o['childs']['list'], l2, o['key'])
            print '- child map end'

        


        
if __name__ == "__main__":
    socket.setdefaulttimeout(15)
    p = GosuslugiParser()
#    p.map_root()
    p.map_root_extend()
#    p.dump_orgs()
#    p.cleanup_parents()
#    p.update_orgs()
#    p.find_org_parents()
#    p.remove_double_parents()
#    p.map_levels()

#    p.dom_cleanup()
#    p.process_services()
#    p.process_emails()
#    p.process_mx()
#    p.analyze_domains()
#    p.analyze_info()
#    p.analyze_contacts()
#    p.find_latin()