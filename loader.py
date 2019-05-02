#!/usr/bin/python3

import sys
from time import *
import datetime
import pytz
from traceback import *
from collections import OrderedDict as dict
import argparse
import mysql.connector
from bs4 import BeautifulSoup as bs
import requests
import json
from uuid import uuid4,UUID
import psycopg2
from psycopg2.extras import execute_values, execute_batch
import string
from pprint import pprint, pformat
from functools import wraps
import logging
from lxml import etree
import re
import jsonschema
from os.path import join, dirname, abspath
from jsonschema import validate
import jsonref



sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'huckleberry', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'huckleberry', '-l', '7351268','7351268']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'bearberry', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-l', '10132204']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-l', '596805']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'gooseberry', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'elderberry', '-l', '5500000']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-l', '6666761']


def timing(f):
    # only useful with functions with main_class param
    @wraps(f)
    def wrap(*args, **kw):
        if args and type(args[0]) == main_class and args[0].monitor_performance:
            m = args[0]
            ts = time()
            result = f(*args, **kw)
            te = time()
            tm = te-ts
            if f.__name__ not in m.perf:
                m.perf[f.__name__] = [tm, tm, 1]
            else:
                tot, avg, count = m.perf[f.__name__]
                m.perf[f.__name__] = [tot+tm, (tot+tm)/count, count+1]
            m.log.info(f"{f.__name__} took: {round(te-ts,2)} secs")
        else:
            result = f(*args, **kw)
        return result
    return wrap


def get_args():
    """FIXME! briefly describe function

    :returns: 
    :rtype: 

    """
    p = argparse.ArgumentParser()
    p.add_argument('-a', '--ole_host', action='store', required=True, help='Ole host')
    p.add_argument('-b', '--folio_host', action='store', required=True, help='Folio host')
    p.add_argument('-l', '--list', action='store', type=int, nargs='+', help='list of bib ids')
    p.add_argument('-f', '--file', action='store', help='Name of file of bib ids')
    args = p.parse_args()
    if not len([i for i in [args.file, args.list] if i]) == 1:
        print("error: Arguemnts must include one of --list or --file")
        p.print_usage()
        sys.exit(1)
    return args


class main_class:
    def __init__(self):
        self.args = get_args()
        self.ole_host = self.args.ole_host
        self.mdb = None
        self.col = dict()
        self.map = dict()
        self.vern = None
        self.bib_rows = []
        self.bib_row = []
        self.bib_doc = None
        self.folio_host = self.args.folio_host
        self.pdb = None
        self.ins_row = []
        self.ins_doc = dict()
        self.jo = dict()
        self.folio_ref = dict()
        self.bib_ids = []
        self.batch_size = 1000
        self.cur_batch = []
        self.log_file = './loader.log'
        self.reject_filename = './reject.log'
        self.reject_file = None
        self.log = None
        self.perf = None
        self.monitor_performance = True
        self.perf = dict()
        self.log_level = 'info'
        # self.stack_trace = True if self.log_level == 'debug' else False
        self.stack_trace = True
        self.cur_time = local_to_utc(datetime.datetime.now())
        self.user_id = '1ad737b0-d847-11e6-bf26-cec0c932ce01'
        self.holdings_ids = []
        self.oh_rows = []
        self.oh_row = None
        self.oh_dict = None
        self.oi_rows = []
        self.oi_row = None
        self.oi_dict = None
        self.fh_rows = []
        self.fh_row = None
        self.fi_rows = []
        self.fi_row = None
        self.pat = re.compile("<record>.*</record>", re.DOTALL)
        self.vern_pat = re.compile('^\d\d\d-\d\d')
        self.tag_map_filename = './tag_map.txt'
        self.drop_constraints_filename = './%s_drop_constraints.sql' % self.folio_host
        self.add_constraints_filename = './%s_add_constraints.sql' % self.folio_host
        self.drop_indexes_filename = './%s_drop_indexes.sql' % self.folio_host
        self.add_indexes_filename = './%s_add_indexes.sql' % self.folio_host
        self.digits = list(string.digits)
        self.alpha_digits = list('%s%s' % (self.digits,string.ascii_lowercase))
        self.tables = []
        self.create_uuid_table = False
        self.ref_schema_dir = '/home/arnt/dev/folio/import/inventory/latest/'
        self.ref_schema_names = None
        self.ref_schema_dict = None
        self.ref_schema_cols = []
        self.ref_schema_rows = None
        self.uuid = dict()
        self.load_ref_tables = True
        self.ref = dict()
        self.ref_dict = dict()
        self.ref_tables = ['alternative_title_type', 'identifier_type',
                           'contributor_name_type','contributor_type', 
                           'classification_type', 'instance_type',
                           'instance_format', 'mode_of_issuance',
                           'instance_status', 'call_number_type',
                           'holdings_type', 'material_type',
                           'electronic_access_relationship',
                           'statistical_code_type', 'statistical_code',
                           'locinstitution', 'loccampus',
                           'loclibrary', 'location', 'loan_type',
                           'service_point']

             
@timing
def main():
    ts = time()
    m = main_class()
    initialize_logging(m)
    open_reject_file(m)
    m.mdb = mdb_class(m)
    m.pdb = pdb_class(m)
    m.log.info('Time started: %s' % ctime())
    try:
        truncate_rows(m)
        load_reference_data(m)
        fetch_bib_ids(m)
        fetch_col_names(m)
        fetch_tag_maps(m)
        fetch_reference_schema_file_names(m)
        fetch_reference_schemas(m)
        # # drop_constraints(m)
        # # drop_indexes(m)
        load_recs(m)
        # # add_constraints(m)
        # # add_indexes(m)
    except:
        m.log.error("Fatal error", exc_info=m.stack_trace)
    finally:
        # m.mdb.cur.close()
        # m.mdb.con.close()
        # m.pdb.cur.close()
        # m.pdb.con.close()
        # close_reject_file(m)
        te = time()
        log_perf_metrics(m)
        m.log.info('Time ended: %s' % ctime())
        m.log.info('Time taken: %2.3f secs' % (te-ts))
        return m


def log_perf_metrics(m):
    hdr = ['                 Function name', '  Total time', '  Average Time', '  Count']
    a,b,c,d = [str(len(s)) for s in hdr]
    format_str = '%'+a+'s%'+b+'.3f%'+c+'.3f%'+d+'d'
    num_lst = [[k,v[0],v[1],v[2]] for k,v in list(m.perf.items())]
    str_lst = [''.join(hdr)] + [format_str % tuple(l) for l in num_lst]
    m.log.info("Performance times by function:")
    for s in str_lst: m.log.info(s)



##### bibs


@timing
def fetch_bib_ids(m):
    # temporarily filter out problem bib_ids
    problems = [4439368, 8367395, 8610487, 8689334, 9137889, 9283082,
                9321922, 9325811, 9791593, 9809578, 10080384, 10135975,
                10246526, 10270139, 10271734, 10320234, 10844383, 11352164,
                11388362, 11446149]
    if m.args.list:
        m.bib_ids = create_batches(m, m.args.list)
    elif m.args.file:
        with open(m.args.file, 'r') as f:
            lst = f.readlines()
            lst = [int(i.strip()) for i in lst if i.strip() not in ('bib_id','')]
            lst = list(set(lst)-set(problems))
            m.bib_ids = create_batches(m, lst)
    m.log.debug("fetched bib_ids")


@timing
def load_recs(m):
    btime = time()
    bib_count = holdings_count = item_count = 0
    for batch in m.bib_ids:
        m.cur_batch = batch
        fetch_bib_rows(m)
        map_bib_rows(m)
        save_ins_rows(m)
        bib_count = bib_count + len(m.cur_batch)
        m.log.info("bibs processed: %d" % bib_count)
        
        fetch_holdings_rows(m)
        fetch_holdings_note_rows(m)
        fetch_holdings_uri_rows(m)
        fetch_holdings_statistical_code_rows(m)
        fetch_extent_of_ownership_rows(m)
        map_holdings_rows(m)
        save_holdings_rows(m)
        holdings_count = holdings_count + len(m.oh_rows)
        m.log.debug("holdings processed: %d" % holdings_count)

        fetch_item_rows(m)
        fetch_item_former_ids(m)
        fetch_item_note_rows(m)
        fetch_item_statistical_code_rows(m)
        map_item_rows(m)
        save_item_rows(m)
        item_count = item_count + len(m.oi_rows)
        m.log.debug("item processed: %d" % item_count)
    m.log.info("completed loading recs")
    

@timing
def fetch_bib_rows(m):
    m.bib_rows = []
    m.ins_rows = []
    cols = m.col['ole_ds_bib_t'] + ['uuid']
    s = ','.join(['%s'] * len(m.cur_batch))
    sql = "select b.*, u.uuid from ole_ds_bib_t b " \
          "join cat.uuid u on b.bib_id = u.id " \
          "where u.table_name = 'ole_ds_bib_t' and b.bib_id in (%s) " % s
    m.mdb.cur.execute(sql, m.cur_batch)
    m.bib_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
    m.log.debug("fetched bib rows")


@timing
def map_bib_rows(m):
    for row in m.bib_rows:
        try:
            m.bib_row = row
            map_bib_doc(m)
            create_ins_doc(m)
            create_ins_row(m)
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (row['bib_id'], None, None))
            m.log.error('unable to map bib row: %d' % m.bib_row['bib_id'],exc_info=m.stack_trace)
            m.log.info(pformat(row))
            if row['bib_id'] in m.cur_batch: m.cur_batch.remove(row['bib_id'])
            continue


def map_marc_in_json(m):
    # Find out where to store this
    j = dict([['id', m.bib_row['uuid']], ['content', dict()]])
    for a in m.bib_doc:
        if a.tag in ['leader']:
            j['content']['leader'] = str(a.text)
            j['content']['fields'] = []
        elif a.tag in ['controlfield']:
            tag, txt = (a.attrib['tag'], str(a.text))
            d = dict([[tag, txt]])
            j['content']['fields'].append(d)
        elif a.tag in ['datafield']:
            tag, ind1, ind2 = (a.attrib['tag'],a.attrib['ind1'], a.attrib['ind2'])
            d = dict([[tag, dict([['ind1', ind1],['ind2', ind2], ['subfields', []]])]])
            for b in a:
                if b.tag in ['subfield']:
                    sf, txt = (b.attrib['code'], str(b.text))
                    subd = dict([[sf, txt]])
                    d[tag]['subfields'].append(subd)
            j['content']['fields'].append(d)
    return j
    
        
# @timing
def map_bib_doc(m):
    m.bib_doc = None
    btime = time()
    for jo in m.jo.keys(): m.jo[jo] = []
    parse_marcxml(m)
    for a in m.bib_doc:
        if a.tag in ['leader', 'controlfield']:
            tag = '000' if a.tag == 'leader' else a.tag
            if tag in m.map:
                ind1, ind2, sf = (None, None, None)
                content = str(a.text)
                acc = [dict([['tag',tag],['ind1',ind1],['ind2',ind2],
                                          ['sf',sf],['content',content]])]
                for jo in m.map[tag]['jo']: m.jo[jo].append(acc)
        elif a.tag in ['datafield']:
            tag = a.attrib['tag']
            if tag in m.map:
                acc = []
                ind1, ind2 = (a.attrib['ind1'], a.attrib['ind2'])
                for b in a:
                    if b.tag in ['subfield']:
                        sf = b.attrib['code']
                        if sf in m.map[tag]['lst']:
                            content = str(b.text)
                            acc.append(dict([['tag',tag],['ind1',ind1],['ind2',ind2],
                                             ['sf',sf],['content',content]]))
                for jo in m.map[tag]['jo']: m.jo[jo].append(acc)


#@timing
def parse_marcxml(m):
    m.bib_doc = etree.fromstring(re.search(m.pat, m.bib_row['content']).group())


#@timing
def create_ins_doc(m):
    m.ins_doc = dict()
    order_lists(m)
    map_vernacular(m)
    add_vernacular(m)
    map_id(m)
    map_hrid(m)
    map_source(m)
    map_title(m)
    map_index_title(m)
    map_alternative_titles(m)
    map_editions(m)
    map_series(m)
    map_identifiers(m)
    map_contributors(m)
    map_subjects(m)
    map_classifications(m)
    map_publications(m)
    map_publication_frequency(m)
    map_publication_range(m)
    map_electronic_access(m)
    map_instance_type(m)
    map_instance_format(m)
    map_physical_descriptions(m)
    map_languages(m)
    map_notes(m)
    map_mode_of_issuance(m)
    map_catalog_date(m)
    map_previously_held(m)
    map_staff_suppress(m)
    map_discovery_suppress(m)
    map_statistical_codes(m)
    map_source_record_format(m)
    map_status(m)
    map_status_update_date(m)
    map_metadata(m)


def order_lists(m):
    # put sf 6 and sf 3 at the head of each list
    # leave the order of the other subfields alone
    # check for ':' at the end of each sf 3
    # create a new function for the latter
    # for clarity and test for performance
    for k in m.jo.keys():
        acc1 = []
        for l in m.jo[k]:
            acc2 = []
            x = y = z = None
            for d in l:
                if d['sf'] == '3':
                    x = d
                    x['content'] = '%s:' % x['content'].strip(':')
                elif d['sf'] == '6': y = d
                elif d['sf'] == '5': z = d
                else: acc2.append(d)
            if x: acc2.insert(0, x)
            if y: acc2.insert(0, y)
            if z: acc2.append(z)
            acc1.append(acc2)
        m.jo[k] = acc1


def map_vernacular(m):
    v = dict()
    for l in m.jo['vernacular']:
        key = None
        for d in l:
            if d['sf'] == '6' and re.search(m.vern_pat, d['content']):
                key = d['content'][:6]
                v[key] = []
            elif key:
                tag = key[:3]
                d['tag'] = tag
                v[key].append(d)
    m.vern = v


# def add_vernacular(m):
#     # for intercollated  880s
#     # add sf5s at end of list
#     for key in m.jo.keys():
#         new_lst = []
#         for l in m.jo[key]:
#             acc = []
#             k = None
#             for d in l:
#                 tag = d['tag']
#                 if d['sf'] in ['6']: k = '%s-%s' % (d['tag'],d['content'][4:6])
#                 else: acc.append(d)
#             new_lst.append(acc)
#             if k in m.vern: new_lst.append(m.vern[k])
#         m.jo[key] = new_lst
        
#     for key in [k for k in m.vern.keys() if k[4:6] == '00']:
#         tag = key[:3]
#         if tag in m.map:
#             for name in m.map[tag]['jo']:
#                 m.jo[name].append(m.vern[key])
                

def add_vernacular(m):
    # for non-intercollated  880s
    for key in m.jo.keys():
        new_lst = []
        for l in m.jo[key]:
            acc = []
            k = None
            for d in l:
                tag = d['tag']
                if d['sf'] in ['6']: k = '%s-%s' % (d['tag'],d['content'][4:6])
                else: acc.append(d)
            new_lst.append(acc)
            # if k in m.vern: new_lst.append(m.vern[k])
        m.jo[key] = new_lst
        
    for key in [k for k in m.vern.keys()]:
    # for key in [k for k in m.vern.keys() if k[4:6] == '00']:
        tag = key[:3]
        if tag in m.map:
            for name in m.map[tag]['jo']:
                m.jo[name].append(m.vern[key])
                

def map_id(m):
    # use bib_id in new model?
    m.ins_doc['id'] = m.bib_row['uuid']

    
def map_hrid(m):
    # from marc 001
    m.ins_doc['hrid'] = m.bib_row['bib_id']

    
def map_source(m):
    # in revised datamodel this value is a list; here it is a string
    m.ins_doc['source'] = 'marc'

    
def map_title(m):
    s = None
    l = (m.jo['title'] + [[],[]])[:2]
    if l[0]: s = ''.join([d['content'] for d in l[0]])
    if l[1]: s = '%s (%s)' % (s, ''.join([d['content'] for d in l[1]]))
    m.ins_doc['title'] = s
        
    
def map_index_title(m):
    # Use the ind2 value to offset the start of the 245
    s = None
    l = (m.jo['title'] + [[],[]])[:2]
    if l[0]: 
        ind = int(l[0][0]['ind2']) if l[0][0]['ind2'] in m.digits else 0
        s = ''.join([d['content'] for d in l[0]])[ind:]
    if l[1]:
        s = '%s (%s)' % (s, ''.join([d['content'] for d in l[1]]))
    m.ins_doc['indexTitle'] = s

    
def map_alternative_titles(m):
    a = dict([['130', 'Uniform Title'],
              ['240', 'Uniform Title'],
              ['246', 'Variant Title'],
              ['247', 'Former Title']]) 
    acc = []
    for l in m.jo['alternativeTitles']:
        uuid = m.ref['alternative_title_type'][a[l[0]['tag']]]
        t = dict([['alternativeTitleTypeId', uuid],
                 ['alternativeTitle', ''.join([d['content'] for d in l])]])
        acc.append(t)
    m.ins_doc['alternativeTitles'] = acc

    
def map_editions(m):
    acc = []
    for l in m.jo['editions']:
        acc.append(''.join([d['content'] for d in l]))
    m.ins_doc['editions'] = acc
    
    
def map_series(m):
    # use 830 if present, otherwise 490, otherwise 440
    acc = []
    for l in m.jo['series']:
        acc.append(''.join([d['content'] for d in l]))
    m.ins_doc['series'] = acc

    
def map_identifiers(m):
    i = {'010':{'a':'LCCN', 'z':'Invalid LCCN'},
         '020':{'a':'ISBN', 'z':'Invalid ISBN'},
         '022':{'a':'ISSN', 'z':'Invalid ISSN', 'l':'Linking ISSN'},
         '024':{'a':'Other Standard Identifier'},
         '028':{'a':'Publisher or Distributor Number'},
         '035':{'a':'System Control Number', 'z':'Cancelled System Control Number'},
         '074':{'a':'GPO Item Number', 'z':'Cancelled GPO Item Number'}}
    acc = []
    for d in [d for l in m.jo['identifiers'] for d in l]:
        if d['tag'] in i and d['sf'] in ('a','z','l'):
                uuid = m.ref['identifier_type'][i[d['tag']][d['sf']]]
                j = {'identifierTypeId': uuid, 'value': d['content']}
                acc.append(j)
    m.ins_doc['identifiers'] = acc
            

def map_contributors(m):
    # Need to work with Christie on conditional logic and cleanup issues
    n = {'100': {'primary': True, 'type': 'Personal name'},
         '110': {'primary': True, 'type': 'Corporate name'},
         '111': {'primary': True, 'type': 'Meeting name'},
         '700': {'primary': False, 'type': 'Personal name'},
         '710': {'primary': False, 'type': 'Corporate name'},
         '711': {'primary': False, 'type': 'Meeting name'}}
    acc = []
    for l in m.jo['contributors']:
        c = {}
        acc2 = []
        for d in l:
            if d['tag'] in n and d['sf'] in ('e','j','4'):
                content = d['content'].capitalize().strip('., ')
                c['primary'] = n[d['tag']]['primary']
                c['contributorNameTypeId'] = m.ref['contributor_name_type'][n[d['tag']]['type']]
                if d['sf'] in ['e'] and content in m.ref['contributor_type']:
                    c['contributorTypeId'] = m.ref['contributor_type'][content]
                elif d['sf'] in ['j'] and content in m.ref['contributor_type']:
                    c['contributorTypeId'] = m.ref['contributor_type'][content]
                elif d['sf'] in ['4'] and d['content'] in m.ref['contributor_type']:
                    c['contributorTypeId'] = m.ref['contributor_type'][d['content']]
                else: c['contributorTypeText'] = d['content']
            else: acc2.append(d['content'])
        c['name'] = ' '.join (acc2)
        acc.append(c)
    m.ins_doc['contributors'] = acc


def map_subjects(m):
    # how to I handle 880s here?
    # 600 - 655 only include if ind2 is 0,2,7, the latter only if sf 2 == 'fast'
    # Only concatenate alphbetic subfields, ie, exclude '0' and '7'.
    acc = []
    for l in m.jo['subjects']:
        tagp = [d for d in l if d['tag'] >= '600' and d['tag'] <='655']
        indp = [d for d in l if tagp and d['ind2'] in ('0','2')]
        fastp = [d for d in l if tagp and d['ind2'] == '7' and d['sf'] == '2' \
                 and d['content'] == 'fast']
        if tagp and indp:
            acc.append('--'.join([d['content'] for d in l if d['sf'] not in m.digits]))
        elif tagp and fastp:
            acc.append('--'.join([d['content'] for d in l if d['sf'] not in m.digits]))
        elif not tagp:
            acc.append('--'.join([d['content'] for d in l if d['sf'] not in m.digits]))
        m.ins_doc['subjects'] = acc


def map_classifications(m):
    a = {'050': 'Library of Congress Classification',
         '082': 'Dewey Decimal Classification',
         '086': 'Government Document Classification',  
         '090': 'Library of Congress Classification (Local)'}
    acc = []
    for l in m.jo['classification']:
        b = l[0]
        if b['tag'] in a:
            c = dict([['classificationNumber', ' '.join([s for s in [d['content'] for d in l]])],
                      ['classificationTypeId',  m.ref['classification_type'][a[b['tag']]]]])
            acc.append(c)
    m.ins_doc['classifications'] = acc


def map_publications(m): ### review with Christie !!!
    # for dateOfPublication, pull from 008 and ignore 260$c and 264$c.
    # In 008, use first date, but if two are present, separate them with hyphen
    # Need to add role based on ind2 in 264 (use only values 0 = 3); use link that Christie is sending
    # see https://www.loc.gov/marc/bibliographic/bd264.html
    role = {'0': 'Production', '1': 'Publication', '2': 'Distribution',
            '3': 'Manufacture', '4': 'Copyright notice date'}
    acc = []
    for l in m.jo['publication']:
        c = {}
        for d in l:
            if d['tag'] in ['260','264'] and d['sf'] in ['a','b','c']:
                if d['sf'] == 'a': c['place'] = d['content'].strip(',.')
                if d['sf'] == 'b': c['publisher'] = d['content'].strip(',.')
                if d['sf'] == 'c': c['dateOfPublication'] = d['content'].strip(',.')
                if d['ind2'] in ['0','1','2','3','4']: c['role'] = role[d['ind2']]
        if c: acc.append(c)
    m.ins_doc['publication'] = acc


def map_publication_frequency(m):
    l = [[''.join([d['content'] for d in l])] for l in m.jo['publicationFrequency']]
    l = [j for k in l for j in k]
    m.ins_doc['publicationFrequency'] = l


def map_publication_range(m):
    l = [[''.join([d['content'] for d in l])] for l in m.jo['publicationRange']]
    l = [j for k in l for j in k]
    m.ins_doc['publicationRange'] = l


def map_electronic_access(m):
    ind = dict([[' ','No information provided'],
                ['0','Resource'],
                ['1','Version of resource'],
                ['2','Related resource'],
                ['8','No display constant generated']])
    acc = []
    for l in m.jo['electronicAccess']:
        e = dict()
        for d in l:
            if d['sf'] == 'u': e['uri'] = d['content']
            if d['sf'] == 'y': e['linkText'] = d['content']
            if d['sf'] == '3': e['materialsSpecification'] = d['content']
            if d['sf'] == 'z': e['publicNote'] = d['content']
            if d['ind2'] in (' ','0','1','2','8'): e['relationshipId'] = ind[d['ind2']]
        acc.append(e)
    m.ins_doc['electronicAccess'] = acc

    
def map_instance_type(m):
    # This is a required field. There is temporary placeholder for nulls.
    ref = m.ref['instance_type']
    for a in m.jo['instance_type']:
        for b in a:
            if b['sf'] == 'b':
                c = b['content']
                m.ins_doc['instanceTypeId'] = ref[c] if c in ref \
                                              else m.ref['instance_type']['txt']
         
def map_instance_format(m):
    acc = []
    ref = m.ref['instance_format']
    for a in m.jo['instance_format']:
        for b in a:
            if b['sf'] == 'b':
                c = b['content']
                if c in ref: acc.append(ref[c])
    m.ins_doc['instanceFormatIds'] = acc


def map_physical_descriptions(m):
    m.ins_doc['physicalDescriptions'] = [' '.join(l) for l in \
                        [[d['content'] for d in l] for l in m.jo['physicalDescriptions']]]


def map_languages(m):
    l = [d['content'] for l in m.jo['languages'] for d in l if d['tag'] in ['041']] or \
        [d['content'][35:38] for l in m.jo['languages'] for d in l if d['tag'] in ['008']]
    m.ins_doc['languages'] = l


def map_notes(m):
    m.ins_doc['notes'] = [' '.join(l) for l in \
                          [[d['content'] for d in l] for l in m.jo['notes']]]
    
 
def map_mode_of_issuance(m):
    # from marc LDR/07
    # see https://www.oclc.org/bibformats/en/fixedfield/blvl.html
    d = dict([['a','Monographic component part'],
              ['b','Serial component part'],
              ['c','Collection'],
              ['d','Subunit'],
              ['i','Integrating resource'],
              ['m','Monograph/Item'],
              ['s','Serial']])
    code = m.jo['mode_of_issuance'][0][0]['content'][7]
    if code in d:
        m.ins_doc['modeOfIssuanceId'] = m.ref['mode_of_issuance'][d[code]]
    else: m.ins_doc['modeOfIssuanceId'] = None

    
def map_catalog_date(m):
    # This should remain null
    m.ins_doc['catalogedDate'] = None

    
def map_previously_held(m):
    # This should be False
    m.ins_doc['previouslyHeld'] = False

    
def map_staff_suppress(m):
    # This should be False
    m.ins_doc['staffSuppress'] = False

    
def map_discovery_suppress(m):
    # map from staff_only in bib rec; convert to True/False
    flag = {'Y':True, 'N':False, None: None}[m.bib_row['staff_only']]
    m.ins_doc['discoverySuppress'] = flag

    
def map_statistical_codes(m):
    # we only have this data for holdings and items
    m.ins_doc['staffSuppress'] = None

    
def map_source_record_format(m):
    # can't find this in the record output; why not?
    m.ins_doc['sourceRecordFormat'] = "MARC"

    
def map_status(m):
    # use instance_status and map from m.bib_row['status']
    # This is a hack to get past all of the bad status codes ### debug
    if m.bib_row['status'] in m.ref['instance_status']:
        m.ins_doc['statusId'] = m.ref['instance_status'][m.bib_row['status']]
    else: m.ins_doc['statusId'] = m.ref['instance_status']['Cataloging complete']

    
def map_status_update_date(m):
    m.ins_doc['statusUpdatedDate'] = local_to_utc(m.bib_row['status_updated_date'])

    
def map_metadata(m):
    m.ins_doc['metadata'] = dict([['createdDate', local_to_utc(m.bib_row['date_created'])],
                                  ['updatedDate', local_to_utc(m.bib_row['date_updated'])],
                                  ['createdByUserId', m.user_id],
                                  ['updatedByUserId', m.user_id]])

    
#@timing
def create_ins_row(m):
    m.ins_row = dict([['_id', m.bib_row['uuid']],
                      ['jsonb', json.dumps(m.ins_doc, default=serialize_datatype)],
                      ['creation_date', local_to_utc(m.bib_row['date_created'])],
                      ['created_by', m.user_id],
                      ['instancestatusid', None],
                      ['modeofissuanceid', None],
                      ['bib_id', m.bib_row['bib_id']]])
    m.ins_rows.append(m.ins_row)


@timing
def save_ins_rows(m):
    cols = m.col['instance']
    sql = "insert into instance values %s"
    rows = [list(r.values())[:-1] for r in m.ins_rows]
    count = len(m.ins_rows)
    if not count: return
    template = '(%s)' % ', '.join(['%s'] * len(cols))
    try:
        execute_values(m.pdb.cur, sql, rows, template=template, page_size=count)
        m.pdb.cur.execute("commit")
        m.log.debug("saved instance rows")
    except:
        m.pdb.cur.execute("rollback")
        sql = "insert into instance values (%s)" % ','.join(['%s'] * len(cols))
        for row in m.ins_rows:
            try:
                m.pdb.cur.execute(sql, list(row.values())[:-1])
                m.pdb.cur.execute("commit")
            except:
                m.pdb.cur.execute("rollback")
                row['jsonb'] = json.loads(row['jsonb'])
                m.reject_file.write('%s\t%s\t%s\n' % (row['bib_id'], None, None))
                m.log.error("error in instance row %s" % row['bib_id'], exc_info=m.stack_trace)
                m.log.info(pformat(row))
                if row['bib_id'] in m.cur_batch: m.cur_batch.remove(row['bib_id'])
                continue
        

##### holdings


@timing
def fetch_holdings_rows(m):
    m.oh_rows = []
    m.fh_rows = []
    m.oh_dict = None
    if not m.cur_batch: return
    s = ','.join(['%s'] * len(m.cur_batch))
    l = "','".join(exclude_locations(m))
    cols = m.col['ole_ds_holdings_t'] + ['uuid', 'bib_uuid']
    sql = "select h.*, u.uuid, v.uuid from ole_ds_holdings_t h " \
          "join cat.uuid u on h.holdings_id = u.id " \
          "join ole_ds_bib_t b on h.bib_id = b.bib_id " \
          "join cat.uuid v on b.bib_id = v.id " \
          "where h.location not in ('%s') " \
          "and u.table_name = 'ole_ds_holdings_t' " \
          "and v.table_name = 'ole_ds_bib_t' " \
          "and b.bib_id in (%s) " % (l, s)
    m.mdb.cur.execute(sql, m.cur_batch)
    m.oh_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
    m.oh_dict = dict([[r['holdings_id'],r] for r in m.oh_rows])
    m.log.debug("fetched %d holdings rows" % len(m.oh_rows))


@timing
def fetch_holdings_note_rows(m):
    if m.oh_rows:
        holdings_ids = [r['holdings_id'] for r in m.oh_rows]
        cols = m.col['ole_ds_holdings_note_t']
        s = ','.join(['%s'] * len(holdings_ids))
        sql = "select * from ole_ds_holdings_note_t where holdings_id in (%s)" % s
        m.mdb.cur.execute(sql, holdings_ids)
        note_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for d in m.oh_rows: d['notes'] = []
        for d in note_rows:
            if d['note']: m.oh_dict[d['holdings_id']]['notes'].append(d)
        m.log.debug("fetched %d holdings note rows" % len(note_rows))


@timing
def fetch_holdings_uri_rows(m):
    if m.oh_rows:
        holdings_ids = [r['holdings_id'] for r in m.oh_rows]
        cols = m.col['ole_ds_holdings_uri_t']
        s = ','.join(['%s'] * len(holdings_ids))
        sql = "select * from ole_ds_holdings_uri_t where holdings_id in (%s)" % s
        m.mdb.cur.execute(sql, holdings_ids)
        uri_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for d in m.oh_rows: d['uris'] = []
        for d in uri_rows:
            if d['uri']: m.oh_dict[d['holdings_id']]['uris'].append(d)
        m.log.debug("fetched %d holdings uri rows" % len(uri_rows))


@timing
def fetch_holdings_statistical_code_rows(m):
    if m.oh_rows:
        holdings_ids = [r['holdings_id'] for r in m.oh_rows]
        cols = m.col['ole_ds_holdings_stat_search_t']
        s = ','.join(['%s'] * len(holdings_ids))
        sql = "select * from ole_ds_holdings_stat_search_t where holdings_id in (%s)" % s
        m.mdb.cur.execute(sql, holdings_ids)
        stat_cd_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for d in m.oh_rows: d['statistical_codes'] = []
        for d in stat_cd_rows:
            if d['stat_search_code_id']: m.oh_dict[d['holdings_id']]['statistical_codes'].append(d)
        m.log.debug("fetched %d statistical_codes rows" % len(stat_cd_rows))


@timing
def fetch_extent_of_ownership_rows(m):
    if m.oh_rows:
        holdings_ids = [r['holdings_id'] for r in m.oh_rows]
        cols = m.col['ole_ds_ext_ownership_t']
        s = ','.join(['%s'] * len(holdings_ids))
        sql = "select * from ole_ds_ext_ownership_t where holdings_id in (%s)" % s
        m.mdb.cur.execute(sql, holdings_ids)
        ext_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        ext_dict = dict([[d['ext_ownership_id'], d] for d in ext_rows])

        if ext_rows:
            ext_ids = [d['ext_ownership_id'] for d in ext_rows]
            cols = m.col['ole_ds_ext_ownership_note_t']
            s = ','.join(['%s'] * len(ext_ids))
            sql = "select * from ole_ds_ext_ownership_note_t where ext_ownership_id in (%s)" % s
            m.mdb.cur.execute(sql, ext_ids)
            ext_note_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
            for d in ext_rows: d['notes'] = []
            for d in ext_note_rows:
                if d['note']: ext_dict[d['ext_ownership_id']]['notes'].append(d)
        
        for d in m.oh_rows: d['ext_ownership'] = []
        for d in ext_rows:
            if d['text']: m.oh_dict[d['holdings_id']]['ext_ownership'].append(d)
        m.log.debug("fetched %d extent of ownership rows" % len(ext_rows))


def add_json_metadata(m):
    date = datetime_to_str(m.cur_time)
    d = {k:v for k,v in [['createdDate', date], ['updatedDate', date],
                         ['createdByUserId', m.user_id], ['updatedByUserId', m.user_id]]}
    return d


@timing
def map_holdings_rows(m):
    for row in m.oh_rows:
        # try:
        m.oh_row = row
        create_holdings_doc(m)
        create_holdings_row(m)
        # except:
        #     m.reject_file.write('%s\t%s\t%s\n' % (row['bib_id'], row['holdings_id'], None))
        #     m.log.error('unable to map holdings: %d' % row['holdings_id'],exc_info=m.stack_trace)
        #     m.log.info(pformat(row))
        #     if row['bib_id'] in m.cur_batch: m.cur_batch.remove(row['bib_id'])
        #     continue


#@timing
def create_holdings_doc(m):
    m.fh_doc = dict()
    map_holdings_id(m)
    map_holdings_instance_id(m)
    map_holdings_hrid(m)
    map_holdings_type_id(m)
    map_holdings_former_ids(m)
    map_holdings_permanent_location(m)
    map_holdings_temporary_location(m)
    map_holdings_call_number_type(m)
    map_holdings_call_number_prefix(m)
    map_holdings_call_number(m)
    map_holdings_call_number_suffix(m)
    map_holdings_shelving_title(m)
    map_holdings_acquisition_format(m)
    map_holdings_acquisition_method(m)
    map_holdings_notes(m)
    map_holdings_ill_policy_id(m)
    map_holdings_retention_policy(m)
    map_holdings_digitization_policy(m)
    map_holdings_electronic_access(m)
    map_holdings_statistical_codes(m)
    map_holdings_statements(m)
    map_holdings_metadata(m)

    
def map_holdings_id(m):
    """
    :desc: map uuid to holdings record id
    :source: ole_ds_holdings_t.uuid
    :target: holdingsrecord.id
    :note: uuid pulled from permanently stored uuid in OLE
    """
    m.fh_doc['id'] = m.oh_row['uuid']


def map_holdings_instance_id(m):
    """
    :desc: map previous holdings ids to Folio formerIds
    :source: (str) ole_ds_holdings_t.former_holdings_id
    :target: (list) holdingsrecord.formerIds
    :note: With OLE, there is only a single value to migrate, unless we include the OLE holdings_id
    """
    m.fh_doc['instanceId'] = m.oh_row['bib_uuid']


def map_holdings_hrid(m):
    """
    :desc: map OLE holdings id to Folio hrid
    :source: ole_ds_holdings_t.holdings_id
    :target: holdingsrecord.hrid
    :note: Find and set the starting number as max(hrid)+1 for the system-assigned hrid.
    """
    m.fh_doc['hrid'] = m.oh_row['holdings_id']


def map_holdings_type_id(m):
    """
    :desc: map OLE holdings type to Folio holdings type
    :source: ole_ds_holdings_t.holdings_type
    :target: holdindgsrecord.holdingsTypeId
    :trans: 'electronic' to 'electronic' and 'print' to 'physical'
    """
    d = {'print':'physical','electronic':'electronic'}
    if m.oh_row['holdings_type'] in d:
        m.fh_doc['holdingsTypeId']=m.ref['holdings_type'][d[m.oh_row['holdings_type']]]
    else: m.fh_doc['holdingsTypeId'] = None


def map_holdings_former_ids(m):
    """
    :desc: map previous holdings ids to Folio formerIds
    :source: (str) ole_ds_holdings_t.former_holdings_id
    :target: (list) holdingsrecord.formerIds
    :note: With OLE, there is only a single value to migrate, unless we include the OLE holdings_id
    """
    m.fh_doc['formerIds'] = [m.oh_row['former_holdings_id']]

    
def map_holdings_permanent_location(m):
    d = {'ASR': 1, 'JRL': 2, 'JCL': 3, 'SPCL': 4, 'ITS': 5, 'Online': 6, 'UCX': 7, 'AANet': 8,
           'Eck': 9, 'SSAd': 10, 'DLL': 11, 'MCS': 12, 'POLSKY': 13, 'LMC': 14, 'unk': 15}
    s = m.oh_row['location']
    # if s and len(s.split('/')) == 3:
    #     ins, lib, shv = s.split('/')
    #     loc = shv + '-' + str(d[lib])
    #     m.fh_doc['permanentLocationId'] = m.ref['location'][loc]
    # else: m.fh_doc['permanentLocationId'] = m.ref['location']['Gen2']

    m.fh_doc['permanentLocationId'] = m.ref['location']['Gen-2']

    
def map_holdings_temporary_location(m):
    m.fh_doc['temporaryLocationId'] = None


def map_holdings_call_number_type(m):
    d = {1: 'No information provided',
         2: 'Library of Congress classification',
         3: 'Dewey Decimal classification',
         4: 'National Library of Medicine classification',
         5: 'Superintendent of Documents classification',
         6: 'Shelving control number',
         7: 'Title',
         8: 'Shelved separately',
         9: 'Source specified in subfield $2',
         10: 'Other scheme'}
    name = d[m.oh_row['call_number_type_id']]
    m.fh_doc['callNumberTypeId'] = m.ref['call_number_type'][name]


def map_holdings_call_number_prefix(m):
    m.fh_doc['callNumberPrefix'] = m.oh_row['call_number_prefix']


def map_holdings_call_number(m):
    m.fh_doc['callNumber'] = m.oh_row['call_number']


def map_holdings_call_number_suffix(m):
    m.fh_doc['callNumberSuffix'] = None


def map_holdings_shelving_title(m):
    m.fh_doc['shelvingTitle'] = None


def map_holdings_acquisition_format(m):
    m.fh_doc['acquisitionFormat'] = None


def map_holdings_acquisition_method(m):
    m.fh_doc['acquisitionMethod'] = None


def map_holdings_notes(m):
    # staff_only defaults to False
    acc = []
    r = m.oh_row
    for d in r['notes']:
        if r['holdings_type'] == 'physical':
            j = dict([['holdingsNoteTypeId', None],
                      ['note', d['note']],
                      ['staffOnly', True if d['type'] == 'non-public' else False]])
        elif r['holdings_type'] == 'electronic' and d['type'] == 'non-public':
            j = dict([['holdingsNoteTypeId', None],
                      ['note', d['note']],
                      ['staffOnly', True]])
            acc.append(j)
    m.fh_doc['notes'] = acc
    
    
def map_holdings_ill_policy_id(m):
    m.fh_doc['illPolicyId'] = None


def map_holdings_retention_policy(m):
    m.fh_doc['retentionPolicy'] = None


def map_holdings_digitization_policy(m):
    m.fh_doc['digitizationPolicy'] = None


def map_holdings_electronic_access(m):
    ind = dict([[' ','No information provided'],
                ['0','Resource'],
                ['1','Version of resource'],
                ['2','Related resource'],
                ['8','No display constant generated']])
    acc = []
    for l in m.jo['electronicAccess']:
        uri_from_holdings = [u['uri'] for u in m.oh_row['uris']]
        uri_from_856 = [d['content'] for d in l if d['sf'] == 'u']
        if set(uri_from_856).intersection(set(uri_from_holdings)):
            j = dict()
            for d in l:
                if d['sf'] == 'u': j['uri'] = d['content']
                if d['sf'] == 'y': j['linkText'] = d['content']
                if d['sf'] == '3': j['materialsSpecification'] = d['content']
                if d['sf'] == 'z': j['publicNote'] = d['content']
                if d['ind2'] in (' ','0','1','2','8'):
                    j['relationshipId'] = m.ref['electronic_access_relationship'][ind[d['ind2']]]
            acc.append(j)
    m.fh_doc['electronicAccess'] = acc

    
def map_holdings_statistical_codes(m):
    d = dict([[1, 'books'],
              [2, 'ebooks'],
              [3, 'music'],
              [4, 'emusic'],
              [5, 'rmusic'],
              [6, 'rnonmusic'],
              [7, 'visual'],
              [8, 'mmedia'],
              [9, 'compfiles'],
              [10, 'mfilm'],
              [11, 'mfiche'],
              [12, 'maps'],
              [13, 'emaps'],
              [14, 'serials'],
              [15, 'eserials'],
              [16, 'arch'],
              [17, 'its'],
              [18, 'mss'],
              [51, 'vidstream'],
              [52, 'audstream'],
              [53, 'withdrawn'],
              [54, 'evisual'],
              [56, 'polsky']])
    l = [d[a['stat_search_code_id']] for a in m.oh_row['statistical_codes']]
    m.fh_doc['statisticalCodeIds'] = [m.ref['statistical_code'][a] for a in l]


def map_holdings_statements(m):
    hst, hsti, hsfs, extp = ['holdingsStatements', 'holdingsStatementsForIndexes', 
                             'holdingsStatementsForSupplements', 'ext_ownership_type_id']
    for s in [hst, hsti, hsfs]: m.fh_doc[s] = []
    for d in m.oh_row['ext_ownership']:
        notes = ' '.join(['%s (%s)' % (a['note'], a['type']) for a in d['notes']])
        statement = dict([['statement', d['text']], ['note', notes]])
        if d[extp] == 1: m.fh_doc[hst].append(statement)
        elif d[extp] == 2: m.fh_doc[hsti].append(statement)
        elif d[extp] == 3: m.fh_doc[hsfs].append(statement)


def map_holdings_metadata(m):
    m.fh_doc['metadata'] = add_json_metadata(m)

    
#@timing
def create_holdings_row(m):
    m.fh_row = dict([['_id', m.oh_row['uuid']],
                     ['jsonb', json.dumps(m.fh_doc, default=serialize_datatype)],
                     ['creation_date', local_to_utc(m.oh_row['date_created'])],
                     ['created_by', m.user_id],
                     ['instanceid', m.fh_doc['instanceId']],
                     # ['permanentlocationid', None],
                     ['permanentlocationid', m.fh_doc['permanentLocationId']],
                     ['temporarylocationid', None],
                     ['holdingstypeid', m.fh_doc['holdingsTypeId']],
                     ['callnumbertypeid', m.fh_doc['callNumberTypeId']],
                     ['illpolicyid', m.fh_doc['illPolicyId']]])
    m.fh_rows.append(m.fh_row)


@timing
def save_holdings_rows(m):
    cols = m.col['holdings_record']
    sql = "insert into holdings_record values %s"
    rows = [list(r.values()) for r in m.fh_rows]
    count = len(m.fh_rows)
    if not count: return
    try:
        execute_values(m.pdb.cur, sql, rows, page_size=count)
        m.pdb.cur.execute("commit")
        m.log.debug("saved holdings rows")
    except:
        m.pdb.cur.execute("rollback")
        sql = "insert into holdings_record values (%s)" % ','.join(['%s'] * len(cols))
        for row in m.fh_rows:
            m.fh_row = row
            try:
                m.pdb.cur.execute(sql, list(row.values())[:-2])
                m.pdb.cur.execute("commit")
            except:
                m.pdb.cur.execute("rollback")
                row['jsonb'] = json.loads(row['jsonb'])
                m.reject_file.write('%s\t%s\t%s\n' % (row['bib_id'], row['holdings_id'], None))
                m.log.error("error in holdings row %s" % row['holdings_id'], exc_info=m.stack_trace)
                m.log.info(pformat(row))
                if row['bib_id'] in m.cur_batch: m.cur_batch.remove(row['bib_id'])
                continue

            
##### items


@timing
def fetch_item_rows(m):
    if not m.cur_batch: return
    m.oi_rows = []
    m.fi_rows = []
    s = ','.join(['%s'] * len(m.cur_batch))
    l = "','".join(exclude_locations(m))
    cols = m.col['ole_ds_item_t'] + ['uuid', 'holdings_uuid']
    sql = "select i.*, u.uuid, v.uuid from ole_ds_item_t i " \
          "join ole_ds_holdings_t h on i.holdings_id = h.holdings_id " \
          "join ole_ds_bib_t b on h.bib_id = b.bib_id " \
          "join cat.uuid u on i.item_id = u.id " \
          "join cat.uuid v on h.holdings_id = v.id " \
          "where h.location not in ('%s') " \
          "and i.location not in ('%s') " \
          "and u.table_name = 'ole_ds_item_t' " \
          "and v.table_name = 'ole_ds_holdings_t' " \
          "and b.bib_id in (%s) " % (l, l, s)
    m.mdb.cur.execute(sql, m.cur_batch)
    m.oi_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
    m.oi_dict = dict([[r['item_id'],r] for r in m.oi_rows])
    m.log.debug("fetched %d item rows" % len(m.oi_rows))
    fetch_item_note_rows(m)


@timing
def fetch_item_former_ids(m):
    if m.oi_rows:
        item_ids = [r['item_id'] for r in m.oi_rows]
        cols = m.col['ole_ds_itm_former_identifier_t']
        s = ','.join(['%s'] * len(item_ids))
        sql = "select * from ole_ds_itm_former_identifier_t where item_id in (%s)" % s
        m.mdb.cur.execute(sql, item_ids)
        former_id_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for d in m.oi_rows: d['former_ids'] = []
        for d in former_id_rows:
            if d['value']: m.oi_dict[d['item_id']]['former_ids'].append(d['value'])
        m.log.debug("fetched %d item former_id rows" % len(former_id_rows))


@timing
def fetch_item_note_rows(m):
    if m.oi_rows:
        item_ids = [r['item_id'] for r in m.oi_rows]
        cols = m.col['ole_ds_item_note_t']
        s = ','.join(['%s'] * len(item_ids))
        sql = "select * from ole_ds_item_note_t where item_id in (%s)" % s
        m.mdb.cur.execute(sql, item_ids)
        note_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for d in m.oi_rows: d['notes'] = []
        for d in note_rows:
            if d['note']: m.oi_dict[d['item_id']]['notes'].append(d)
        m.log.debug("fetched %d item note rows" % len(note_rows))

    
@timing
def fetch_item_statistical_code_rows(m):
    if m.oi_rows:
        item_ids = [r['item_id'] for r in m.oi_rows]
        cols = m.col['ole_ds_item_stat_search_t']
        s = ','.join(['%s'] * len(item_ids))
        sql = "select * from ole_ds_item_stat_search_t where item_id in (%s)" % s
        m.mdb.cur.execute(sql, item_ids)
        stat_cd_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for d in m.oi_rows: d['statistical_codes'] = []
        for d in stat_cd_rows:
            if d['stat_search_code_id']: m.oi_dict[d['item_id']]['statistical_codes'].append(d)
        m.log.debug("fetched %d statistical_codes rows" % len(stat_cd_rows))


@timing
def map_item_rows(m):
    for row in m.oi_rows:
        try:
            m.oi_row = row
            create_item_doc(m)
            create_item_row(m)
        except:
            m.reject_file.write('%d\t%d\t%d\n' % (row['bib_id'], row['holdings_id'], row['item_id']))
            m.log.error('unable to map item: %d' % row['item_id'], exc_info=m.stack_trace)
            m.log.info(pformat(row))
            if row['bib_id'] in m.cur_batch: m.cur_batch.remove(row['bib_id'])
            continue


#@timing
def create_item_doc(m):
    m.fi_doc = dict()
    map_item_id(m)
    # map_item_hrid(m)
    map_item_holdings_id(m)
    map_item_former_ids(m)
    map_item_discovery_suppress(m)
    map_item_accession_number(m)
    map_item_barcode(m)
    map_item_level_call_number(m)
    map_item_level_call_number_prefix(m)
    map_item_level_call_number_suffix(m)
    map_item_level_call_number_type(m)
    map_item_volume(m)
    map_item_enumeration(m)
    map_item_chronology(m)
    map_item_year_caption(m)
    map_item_identifier(m)
    map_item_copy_numbers(m)
    map_item_num_of_pieces(m)
    map_item_description_of_pieces(m)
    map_item_number_of_missing_pieces(m)
    map_item_missing_pieces(m)
    map_item_missing_pieces_date(m)
    map_item_item_damaged_status_id(m)
    map_item_item_damaged_status_date(m)
    map_item_notes(m)
    # map_item_circulation_notes(m) # not yet implemented
    map_item_status(m)
    map_item_material_type(m)
    map_item_permanent_loan_type(m)
    map_item_temporary_loan_type(m)
    map_item_permanent_location(m)
    map_item_temporary_location(m)
    map_item_electronic_access(m)
    map_item_intransit_service_point(m)
    map_item_statistical_codes(m)
    # map_item_purchase_order_id(m)
    map_item_metadata(m)

    
def map_item_id(m):
    m.fi_doc['id'] = m.oi_row['uuid']


def map_item_hrid(m):
    m.fi_doc['hrid'] = str(m.oi_row['item_id'])


def map_item_holdings_id(m):
    m.fi_doc['holdingsRecordId'] = m.oi_row['holdings_uuid']


def map_item_former_ids(m):
    m.fi_doc['formerIds'] = m.oi_row['former_ids']


def map_item_discovery_suppress(m):
    val = {'Y':True, 'N':False, None: None, ' ':None, '':None }[m.oi_row['staff_only']]
    m.fi_doc['discoverySuppress'] = val


def map_item_accession_number(m):
    m.fi_doc['accessionNumber'] = None


def map_item_barcode(m):
    m.fi_doc['barcode'] = m.oi_row['barcode']


def map_item_level_call_number(m):
    m.fi_doc['itemLevelCallNumber'] = m.oi_row['call_number']


def map_item_level_call_number_prefix(m):
    m.fi_doc['itemLevelCallNumberPrefix'] = m.oi_row['call_number_prefix']


def map_item_level_call_number_suffix(m):
    m.fi_doc['itemLevelCallNumberSuffix'] = None


def map_item_level_call_number_type(m):
    d = {1: 'No information provided',
         2: 'Library of Congress classification',
         3: 'Dewey Decimal classification',
         4: 'National Library of Medicine classification',
         5: 'Superintendent of Documents classification',
         6: 'Shelving control number',
         7: 'Title',
         8: 'Shelved separately',
         9: 'Source specified in subfield $2',
         10: 'Other scheme'}
    name = d[m.oi_row['call_number_type_id']] if 'call_number_type_id' in d else None
    m.fi_doc['itemLevelCallNumberTypeId'] = m.ref['call_number_type'][name] if name else None


def map_item_volume(m):
    m.fi_doc['volume'] = None


def map_item_enumeration(m):
    m.fi_doc['enumeration'] = m.oi_row['enumeration']


def map_item_chronology(m):
    m.fi_doc['chronology'] = m.oi_row['chronology']


def map_item_year_caption(m):
    m.fi_doc['yearCaption'] = []


def map_item_identifier(m):
    m.fi_doc['itemIdentifier'] = None


def map_item_copy_numbers(m):
    m.fi_doc['copyNumbers'] = [m.oi_row['copy_number']]


def map_item_num_of_pieces(m):
    v = m.oi_row['num_pieces']
    m.fi_doc['numberOfPieces'] = v if isinstance(v, str) and v.strip() and int(v) > 1 else None


def map_item_description_of_pieces(m):
    m.fi_doc['descriptionOfPieces'] = m.oi_row['desc_of_pieces']


def map_item_number_of_missing_pieces(m):
    val = m.oi_row['missing_pieces_count']
    m.fi_doc['numberOfMissingPieces'] = str(val) if isinstance(val, int) else None


def map_item_missing_pieces(m):
    m.fi_doc['missingPieces'] = m.oi_row['missing_pieces']


def map_item_missing_pieces_date(m):
    m.fi_doc['missingPiecesDate'] = m.oi_row['missing_pieces_effective_date']


def map_item_item_damaged_status_id(m):
    # There should be a lookup table for this
    m.fi_doc['itemDamagedStatusId'] = None


def map_item_item_damaged_status_date(m):
    m.fi_doc['itemDamagedStatusDate'] = None


def map_item_notes(m):
    # staff_only defaults to False
    acc = []
    for d in m.oi_row['notes']:
        j = dict([['itemNoteTypeId', None],
                  ['note', d['note']],
                  ['staffOnly', True if d['type'] == 'non-public' else False]])
        acc.append(j)
    m.fi_doc['notes'] = acc
    
    
def map_item_circulation_notes(m):
    # Check with Cheryl about how to type notes as 'check-in' vs 'check-out'
    keys = ['check_in_note', 'claims_returned_note']
    acc = []
    for n in [m.oi_row[k] for k in keys]:
        if n not in [None, ' ', '']:
            j = dict([['noteType', 'check-in'],
                      ['note', n],
                      ['staffOnly', True]])
            acc.append(j)
    m.fi_doc['circulationNotes'] = acc
    
    
def map_item_status(m):
    d = dict([(1, 'ANAL'), (2, 'AVAILABLE'), (4, 'AVAILABLE-AT-MANSUETO'), (35, 'BTAASPR'),
              (25, 'DECLARED-LOST'), (24, 'FLAGGED-FOR-RESERVE'), (28, 'In Process'), (5,'INPROCESS'),
              (6, 'INPROCESS-CRERAR'), (7, 'INPROCESS-LAW'), (8, 'INPROCESS-MANSUETO'),
              (9, 'INPROCESS-REGENSTEIN'), (10, 'INTRANSIT'), (11, 'INTRANSIT-FOR-HOLD'),
              (12, 'INTRANSIT-PER-STAFF-REQUEST'), (13, 'LOANED'), (14, 'LOST'), (32,'LOST-AND-PAID'),
              (15, 'MISSING'), (16, 'MISSING-FROM-MANSUETO'), (27, 'On Order'), (17, 'ONHOLD'),
              (18, 'ONORDER'), (19, 'RECENTLY-RETURNED'), (20, 'RETRIEVING-FROM-MANSUETO'),
              (21, 'RETURNED-DAMAGED'), (22, 'RETURNED-WITH-MISSING-ITEMS'), (23, 'UNAVAILABLE'),
              (26, 'WITHDRAWN'), (36, 'WITHDRAWN-SPR-BTAA')])
    id = m.oi_row['item_status_id']
    dt = datetime_to_str(local_to_utc(m.oi_row['item_status_date_updated']))
    # j = dict([['name', d[id]],['date', dt]]) if id in d else None
    # m.fi_doc['status'] = j
    j = dict([['name', d[id]]]) if id in d else None
    m.fi_doc['status'] = j
    

def map_item_material_type(m):
    m.fi_doc['materialTypeId'] = m.ref['material_type']['unspecified']


def map_item_permanent_loan_type(m):
    cols = ['id', 'name']
    d = dict([[1, 'stks'], [2, 'buo'], [3, 'mus'], [4, 'res2'], [5, 'spcl'], [6, '16mmcam'],
              [7, 'AVSadpt'], [8, 'AVSasis'], [9, 'AVSmic'], [10, 'AVSport'], [11, 'AVSproj'],
              [12, 'AVSscre'], [13, 'batt'], [14, 'boompol'], [15, 'bordirc'], [16, 'cabl'],
              [17, 'camrig'], [18, 'dslr'], [19, 'dslracc'], [20, 'eres'], [21, 'games'],
              [22, 'gaming'], [23, 'grip'], [24, 'hdcam'], [25, 'headph'], [26, 'ilok'],
              [27, 'inhouse'], [28, 'its2adp'], [29, 'its8adp'], [30, 'its8cnf'], [31, 'its8ipd'],
              [32, 'its8lap'], [33, 'lghtmtr'], [34, 'lights'], [35, 'lmc6wk'], [36, 'macadpt'],
              [37, 'mics'], [38, 'micstnd'], [39, 'mixer'], [40, 'monitrs'], [41, 'noncirc'],
              [42, 'online'], [43, 'playbck'], [44, 'projctr'], [45, 'res168'], [46, 'res24'],
              [47, 'res4'], [48, 'res48'], [49, 'res72'], [50, 'sndacc'], [51, 'sndrec'],
              [52, 'spkrs'], [53, 'stilcam'], [54, 'stks14'], [55, 'stks7'], [56, 'tripod'],
              [57, 'vidcam'], [58, 'yerk'], [182, 'illbuo'], [183, 'ill7day'], [184, 'ill7dayrenew'],
              [185, 'ill21day'], [186, 'ill21dayrenew'], [187, 'ill42day'], [188, 'ill42dayrenew'],
              [189, 'ill77day'], [190, 'ill77dayrenew'], [281, 'itspol2adp'], [282, 'itspol8adp'],
              [283, 'itspol8vid'], [284, 'itspol8ipd'], [285, 'itspol8lap']])
    v = m.ref['loan_type'][d[m.oi_row['item_type_id']]] if m.oi_row['item_type_id'] in d else m.ref['loan_type']['stks']
    m.fi_doc['permanentLoanTypeId'] = v

    
def map_item_temporary_loan_type(m):
    cols = ['id', 'name']
    d = dict([[1, 'stks'], [2, 'buo'], [3, 'mus'], [4, 'res2'], [5, 'spcl'], [6, '16mmcam'],
              [7, 'AVSadpt'], [8, 'AVSasis'], [9, 'AVSmic'], [10, 'AVSport'], [11, 'AVSproj'],
              [12, 'AVSscre'], [13, 'batt'], [14, 'boompol'], [15, 'bordirc'], [16, 'cabl'],
              [17, 'camrig'], [18, 'dslr'], [19, 'dslracc'], [20, 'eres'], [21, 'games'],
              [22, 'gaming'], [23, 'grip'], [24, 'hdcam'], [25, 'headph'], [26, 'ilok'],
              [27, 'inhouse'], [28, 'its2adp'], [29, 'its8adp'], [30, 'its8cnf'], [31, 'its8ipd'],
              [32, 'its8lap'], [33, 'lghtmtr'], [34, 'lights'], [35, 'lmc6wk'], [36, 'macadpt'],
              [37, 'mics'], [38, 'micstnd'], [39, 'mixer'], [40, 'monitrs'], [41, 'noncirc'],
              [42, 'online'], [43, 'playbck'], [44, 'projctr'], [45, 'res168'], [46, 'res24'],
              [47, 'res4'], [48, 'res48'], [49, 'res72'], [50, 'sndacc'], [51, 'sndrec'],
              [52, 'spkrs'], [53, 'stilcam'], [54, 'stks14'], [55, 'stks7'], [56, 'tripod'],
              [57, 'vidcam'], [58, 'yerk'], [182, 'illbuo'], [183, 'ill7day'], [184, 'ill7dayrenew'],
              [185, 'ill21day'], [186, 'ill21dayrenew'], [187, 'ill42day'], [188, 'ill42dayrenew'],
              [189, 'ill77day'], [190, 'ill77dayrenew'], [281, 'itspol2adp'], [282, 'itspol8adp'],
              [283, 'itspol8vid'], [284, 'itspol8ipd'], [285, 'itspol8lap']])
    v = m.ref['loan_type'][d[m.oi_row['temp_item_type_id']]] if m.oi_row['temp_item_type_id'] in d else None
    m.fi_doc['temporaryLoanTypeId'] = v


def map_item_permanent_location(m):
    d = {'ASR': 1, 'JRL': 2, 'JCL': 3, 'SPCL': 4, 'ITS': 5, 'Online': 6, 'UCX': 7, 'AANet': 8,
           'Eck': 9, 'SSAd': 10, 'DLL': 11, 'MCS': 12, 'POLSKY': 13, 'LMC': 14, 'unk': 15}
    s = m.oh_row['location']
    # if s and len(s.split('/')) == 3:
    #     ins, lib, shv = s.split('/')
    #     loc = shv + '-' + str(d[lib])
    #     m.fi_doc['permanentLocationId'] = m.ref['location'][loc]
    # else: m.fi_doc['permanentLocationId'] = m.ref['location']['Gen2']

    m.fi_doc['permanentLocationId'] = m.ref['location']['Gen-2']

    
def map_item_temporary_location(m):
    m.fi_doc['temporaryLocationId'] = None


def map_item_electronic_access(m):
    m.fi_doc['electronicAccess'] = None


def map_item_intransit_service_point(m):
    m.fi_doc['inTransitDestinationServicePointId'] = None


def map_item_statistical_codes(m):
    d = dict([[1, 'books'],
              [2, 'ebooks'],
              [3, 'music'],
              [4, 'emusic'],
              [5, 'rmusic'],
              [6, 'rnonmusic'],
              [7, 'visual'],
              [8, 'mmedia'],
              [9, 'compfiles'],
              [10, 'mfilm'],
              [11, 'mfiche'],
              [12, 'maps'],
              [13, 'emaps'],
              [14, 'serials'],
              [15, 'eserials'],
              [16, 'arch'],
              [17, 'its'],
              [18, 'mss'],
              [51, 'vidstream'],
              [52, 'audstream'],
              [53, 'withdrawn'],
              [54, 'evisual'],
              [56, 'polsky']])
    l = [d[a['stat_search_code_id']] for a in m.oi_row['statistical_codes']]
    m.fi_doc['statisticalCodeIds'] = [m.ref['statistical_code'][a] for a in l]


def map_item_purchase_order_id(m):
    m.fi_doc['purchaseOrderLineIdentifier'] = None


def map_item_metadata(m):
    m.fi_doc['metadata'] = dict([['createdDate', local_to_utc(m.oi_row['date_created'])],
                                 ['updatedDate', local_to_utc(m.oi_row['date_updated'])],
                                 ['createdByUserId','1ad737b0-d847-11e6-bf26-cec0c932ce01'],
                                 ['updatedByUserId','1ad737b0-d847-11e6-bf26-cec0c932ce01']])

    
#@timing
def create_item_row(m):
    m.fi_row = dict([['_id', m.oi_row['uuid']],
                     ['jsonb', json.dumps(m.fi_doc, default=serialize_datatype)],
                     ['creation_date', local_to_utc(m.oi_row['date_created'])],
                     ['created_by', m.user_id],
                     ['holdingsrecordid', m.oi_row['holdings_uuid']],
                     ['permanentloantypeid', m.fi_doc['permanentLoanTypeId']],
                     ['temporaryloantypeid', m.fi_doc['temporaryLoanTypeId']],
                     ['materialtypeid', m.fi_doc['materialTypeId']],
                     ['permanentlocationid', m.fi_doc['permanentLocationId']],
                     ['temporarylocationid', m.fi_doc['temporaryLocationId']],
                     ['item_id', m.oi_row['item_id']],
                     ['holdings_id', m.oi_row['holdings_id']],
                     ['bib_id', m.oh_dict[m.oi_row['holdings_id']]['bib_id']]])
    m.fi_rows.append(m.fi_row)


@timing
def save_item_rows(m):
    cols = m.col['item']
    sql = "insert into item values %s"
    rows = [list(r.values())[:-3] for r in m.fi_rows]
    count = len(m.fi_rows)
    if not count: return
    try:
        execute_values(m.pdb.cur, sql, rows, page_size=count)
        m.pdb.cur.execute("commit")
        m.log.debug("saved item rows")
    except:
        m.pdb.cur.execute("rollback")
        sql = "insert into item values (%s)" % ','.join(['%s'] * len(cols))
        for row in m.fi_rows:
            m.fi_row = row
            try:
                m.pdb.cur.execute(sql, list(row.values())[:-3])
                m.pdb.cur.execute("commit")
            except:
                m.pdb.cur.execute("rollback")
                row['jsonb'] = json.loads(row['jsonb'])
                m.reject_file.write('%s\t%s\t%s\n'% (row['bib_id'],row['holdings_id'],row['item_id']))
                m.log.error("error in item row %s" % row['item_id'], exc_info=m.stack_trace)
                m.log.info(pformat(row))
                if row['bib_id'] in m.cur_batch: m.cur_batch.remove(row['bib_id'])
                continue


##### reference
            

def load_reference_data(m):
    load_ref_uuids(m)
    delete_ref_tables(m)
    load_alternative_title_type(m)
    load_identifier_type(m)
    load_contributor_name_type(m)
    load_contributor_type(m)
    load_classification_type(m)
    load_instance_type(m)
    load_instance_format(m)
    load_mode_of_issuance(m)
    load_instance_status(m)
    load_call_number_type(m)
    load_holdings_type(m)
    load_material_type(m)
    load_electronic_access_relationship(m)
    load_statistical_code_type(m)
    load_statistical_code(m)
    load_locinstitution(m)
    load_loccampus(m)
    load_loclibraries(m)
    load_service_points(m)
    load_locations(m)
    load_loan_types(m)


def delete_ref_tables(m):
    if m.load_ref_tables:
        for t in reversed(m.ref_tables): 
            sql = f"delete from {t};"
            m.pdb.cur.execute(sql)
            m.pdb.cur.execute('commit')


def load_ref_uuids(m):
    for t in m.ref_tables: m.uuid[t] = dict(fetch_uuids(m, t))

        
def load_alternative_title_type(m):
    sql = "delete from alternative_title_type"
    m.pdb.cur.execute(sql)
    m.pdb.cur.execute('commit')
    tbl = 'alternative_title_type'
    cols = ['id','name', 'source']
    rows = [[1, 'Uniform Title', 'local'],
            [2, 'Variant Title', 'local'],
            [3, 'Former Title', 'local']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_identifier_type(m):
    tbl = 'identifier_type'
    cols = ['id','name']
    rows = [[1, 'LCCN'],
            [2, 'ISBN'],
            [3, 'Invalid ISBN'],
            [4, 'ISSN'],
            [5, 'Invalid ISSN'],
            [6, 'Linking ISSN'],
            [7, 'Other Standard Identifier'],
            [8, 'Publisher or Distributor Number'],
            [9, 'System Control Number'],
            [10, 'Cancelled System Control Number'],
            [11, 'GPO Item Number'],
            [12, 'Cancelled GPO Item Number']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_contributor_name_type(m):
    tbl = 'contributor_name_type'
    cols = ['id','name']
    rows = [[1, 'Personal name'],
            [2, 'Corporate name'],
            [3, 'Meeting name']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_contributor_type(m):
    tbl = 'contributor_type'
    cols = ['id','code', 'name', 'source']
    rows = [[1, 'exp', 'Expert', 'marcrelator'],
            [2, 'fmd', 'Film director', 'marcrelator'],
            [3, 'dis', 'Dissertant', 'marcrelator'],
            [4, 'prm', 'Printmaker', 'marcrelator'],
            [5, 'fmp', 'Film producer', 'marcrelator'],
            [6, 'sng', 'Singer', 'marcrelator'],
            [7, 'pbl', 'Publisher', 'marcrelator'],
            [8, 'cll', 'Calligrapher', 'marcrelator'],
            [9, 'com', 'Compiler', 'marcrelator'],
            [10, 'dtc', 'Data contributor', 'marcrelator'],
            [11, 'ppt', 'Puppeteer', 'marcrelator'],
            [12, 'gis', 'Geographic information specialist', 'marcrelator'],
            [13, 'cte', 'Contestee-appellee', 'marcrelator'],
            [14, 'lit', 'Libelant-appellant', 'marcrelator'],
            [15, 'app', 'Applicant', 'marcrelator'],
            [16, 'pra', 'Praeses', 'marcrelator'],
            [17, 'drm', 'Draftsman', 'marcrelator'],
            [18, 'dsr', 'Designer', 'marcrelator'],
            [19, 'ptf', 'Plaintiff', 'marcrelator'],
            [20, 'mon', 'Monitor', 'marcrelator'],
            [21, 'dub', 'Dubious author', 'marcrelator'],
            [22, 'enj', 'Enacting jurisdiction', 'marcrelator'],
            [23, 'fds', 'Film distributor', 'marcrelator'],
            [24, 'prd', 'Production personnel', 'marcrelator'],
            [25, 'cmm', 'Commentator', 'marcrelator'],
            [26, 'aqt', 'Author in quotations or text abstracts', 'marcrelator'],
            [27, 'fmk', 'Filmmaker', 'marcrelator'],
            [28, 'fpy', 'First party', 'marcrelator'],
            [29, 'cmp', 'Composer', 'marcrelator'],
            [30, 'hst', 'Host', 'marcrelator'],
            [31, 'cre', 'Creator', 'marcrelator'],
            [32, 'ctg', 'Cartographer', 'marcrelator'],
            [33, 'lso', 'Licensor', 'marcrelator'],
            [34, 'ltg', 'Lithographer', 'marcrelator'],
            [35, 'tyd', 'Type designer', 'marcrelator'],
            [36, 'cpe', 'Complainant-appellee', 'marcrelator'],
            [37, 'stg', 'Setting', 'marcrelator'],
            [38, 'lbr', 'Laboratory', 'marcrelator'],
            [39, 'auc', 'Auctioneer', 'marcrelator'],
            [40, 'cli', 'Client', 'marcrelator'],
            [41, 'adi', 'Art director', 'marcrelator'],
            [42, 'rpt', 'Reporter', 'marcrelator'],
            [43, 'mdc', 'Metadata contact', 'marcrelator'],
            [44, 'crp', 'Correspondent', 'marcrelator'],
            [45, 'spn', 'Sponsor', 'marcrelator'],
            [46, 'rps', 'Repository', 'marcrelator'],
            [47, 'ptt', 'Plaintiff-appellant', 'marcrelator'],
            [48, 'cur', 'Curator', 'marcrelator'],
            [49, 'stl', 'Storyteller', 'marcrelator'],
            [50, 'tlp', 'Television producer', 'marcrelator'],
            [51, 'prs', 'Production designer', 'marcrelator'],
            [52, 'scl', 'Sculptor', 'marcrelator'],
            [53, 'aui', 'Author of introduction, etc.', 'marcrelator'],
            [54, 'pta', 'Patent applicant', 'marcrelator'],
            [55, 'his', 'Host institution', 'marcrelator'],
            [56, 'rev', 'Reviewer', 'marcrelator'],
            [57, 'rtm', 'Research team member', 'marcrelator'],
            [58, 'str', 'Stereotyper', 'marcrelator'],
            [59, 'rse', 'Respondent-appellee', 'marcrelator'],
            [60, 'wdc', 'Woodcutter', 'marcrelator'],
            [61, 'lsa', 'Landscape architect', 'marcrelator'],
            [62, 'drt', 'Director', 'marcrelator'],
            [63, 'rdd', 'Radio director', 'marcrelator'],
            [64, 'wst', 'Writer of supplementary textual content', 'marcrelator'],
            [65, 'inv', 'Inventor', 'marcrelator'],
            [66, 'asg', 'Assignee', 'marcrelator'],
            [67, 'scr', 'Scribe', 'marcrelator'],
            [68, 'sgn', 'Signer', 'marcrelator'],
            [69, 'prt', 'Printer', 'marcrelator'],
            [70, 'cph', 'Copyright holder', 'marcrelator'],
            [71, 'tyg', 'Typographer', 'marcrelator'],
            [72, 'orm', 'Organizer', 'marcrelator'],
            [73, 'elt', 'Electrotyper', 'marcrelator'],
            [74, 'rcd', 'Recordist', 'marcrelator'],

            [75, 'lse', 'Licensee', 'marcrelator'],
            [76, 'prf', 'Performer', 'marcrelator'],
            [77, 'win', 'Writer of introduction', 'marcrelator'],
            [78, 'anl', 'Analyst', 'marcrelator'],
            [79, 'flm', 'Film editor', 'marcrelator'],
            [80, 'dnc', 'Dancer', 'marcrelator'],
            [81, 'chr', 'Choreographer', 'marcrelator'],
            [82, 'bkp', 'Book producer', 'marcrelator'],
            [83, 'vac', 'Voice actor', 'marcrelator'],
            [84, 'dln', 'Delineator', 'marcrelator'],
            [85, 'cnd', 'Conductor', 'marcrelator'],
            [86, 'dft', 'Defendant-appellant', 'marcrelator'],
            [87, 'brl', 'Braille embosser', 'marcrelator'],
            [88, 'arr', 'Arranger', 'marcrelator'],
            [89, 'cos', 'Contestant', 'marcrelator'],
            [90, 'ard', 'Artistic director', 'marcrelator'],
            [91, 'con', 'Conservator', 'marcrelator'],
            [92, 'etr', 'Etcher', 'marcrelator'],
            [93, 'ive', 'Interviewee', 'marcrelator'],
            [94, 'pre', 'Presenter', 'marcrelator'],
            [95, 'evp', 'Event place', 'marcrelator'],
            [96, 'itr', 'Instrumentalist', 'marcrelator'],
            [97, 'med', 'Medium', 'marcrelator'],
            [98, 'sll', 'Seller', 'marcrelator'],
            [99, 'pbd', 'Publishing director', 'marcrelator'],
            [100, 'csp', 'Consultant to a project', 'marcrelator'],
            [101, 'ccp', 'Conceptor', 'marcrelator'],
            [102, 'led', 'Lead', 'marcrelator'],
            [103, 'ppm', 'Papermaker', 'marcrelator'],
            [104, 'prc', 'Process contact', 'marcrelator'],
            [105, 'mcp', 'Music copyist', 'marcrelator'],
            [106, 'pan', 'Panelist', 'marcrelator'],
            [107, 'oth', 'Other', 'marcrelator'],
            [108, 'cpt', 'Complainant-appellant', 'marcrelator'],
            [109, 'sds', 'Sound designer', 'marcrelator'],
            [110, 'stm', 'Stage manager', 'marcrelator'],
            [111, 'org', 'Originator', 'marcrelator'],
            [112, 'ldr', 'Laboratory director', 'marcrelator'],
            [113, 'pro', 'Producer', 'marcrelator'],
            [114, 'ape', 'Appellee', 'marcrelator'],
            [115, 'fac', 'Facsimilist', 'marcrelator'],
            [116, 'crt', 'Court reporter', 'marcrelator'],
            [117, 'prv', 'Provider', 'marcrelator'],
            [118, 'cts', 'Contestee', 'marcrelator'],
            [119, 'ren', 'Renderer', 'marcrelator'],
            [120, 'cwt', 'Commentator for written text', 'marcrelator'],
            [121, 'let', 'Libelee-appellant', 'marcrelator'],
            [122, 'trl', 'Translator', 'marcrelator'],
            [123, 'lee', 'Libelee-appellee', 'marcrelator'],
            [124, 'ths', 'Thesis advisor', 'marcrelator'],
            [125, 'sht', 'Supporting host', 'marcrelator'],
            [126, 'rth', 'Research team head', 'marcrelator'],
            [127, 'fnd', 'Funder', 'marcrelator'],
            [128, 'wac', 'Writer of added commentary', 'marcrelator'],
            [129, 'ins', 'Inscriber', 'marcrelator'],
            [130, 'edm', 'Editor of moving image work', 'marcrelator'],
            [131, 'cot', 'Contestant-appellant', 'marcrelator'],
            [132, 'bjd', 'Bookjacket designer', 'marcrelator'],
            [133, 'aft', 'Author of afterword, colophon, etc.', 'marcrelator'],
            [134, 'sgd', 'Stage director', 'marcrelator'],
            [135, 'csl', 'Consultant', 'marcrelator'],
            [136, 'dst', 'Distributor', 'marcrelator'],
            [137, 'mfr', 'Manufacturer', 'marcrelator'],
            [138, 'wpr', 'Writer of preface', 'marcrelator'],
            [139, 'dtm', 'Data manager', 'marcrelator'],
            [140, 'prp', 'Production place', 'marcrelator'],
            [141, 'lgd', 'Lighting designer', 'marcrelator'],
            [142, 'pat', 'Patron', 'marcrelator'],
            [143, 'rbr', 'Rubricator', 'marcrelator'],
            [144, 'len', 'Lender', 'marcrelator'],
            [145, 'res', 'Researcher', 'marcrelator'],
            [146, 'asn', 'Associated name', 'marcrelator'],
            [147, 'fmo', 'Former owner', 'marcrelator'],
            [148, 'pfr', 'Proofreader', 'marcrelator'],
            [149, 'sad', 'Scientific advisor', 'marcrelator'],
            [150, 'mrb', 'Marbler', 'marcrelator'],
            [151, 'mod', 'Moderator', 'marcrelator'],
            [152, 'rsp', 'Respondent', 'marcrelator'],
            [153, 'abr', 'Abridger', 'marcrelator'],
            [154, 'cst', 'Costume designer', 'marcrelator'],
            [155, 'uvp', 'University place', 'marcrelator'],
            [156, 'tld', 'Television director', 'marcrelator'],
            [157, 'lie', 'Libelant-appellee', 'marcrelator'],
            [158, 'pdr', 'Project director', 'marcrelator'],
            [159, 'jud', 'Judge', 'marcrelator'],
            [160, 'pmn', 'Production manager', 'marcrelator'],
            [161, 'ato', 'Autographer', 'marcrelator'],
            [162, 'plt', 'Platemaker', 'marcrelator'],
            [163, 'pop', 'Printer of plates', 'marcrelator'],
            [164, 'own', 'Owner', 'marcrelator'],
            [165, 'tcd', 'Technical director', 'marcrelator'],
            [166, 'cor', 'Collection registrar', 'marcrelator'],
            [167, 'mtk', 'Minute taker', 'marcrelator'],
            [168, 'sce', 'Scenarist', 'marcrelator'],
            [169, 'crr', 'Corrector', 'marcrelator'],
            [170, 'art', 'Artist', 'marcrelator'],
            [171, 'dbp', 'Distribution place', 'marcrelator'],
            [172, 'sec', 'Secretary', 'marcrelator'],
            [173, 'egr', 'Engraver', 'marcrelator'],
            [174, 'tch', 'Teacher', 'marcrelator'],
            [175, 'mte', 'Metal-engraver', 'marcrelator'],
            [176, 'wam', 'Writer of accompanying material', 'marcrelator'],
            [177, 'pte', 'Plaintiff-appellee', 'marcrelator'],
            [178, 'dfe', 'Defendant-appellee', 'marcrelator'],
            [179, 'bkd', 'Book designer', 'marcrelator'],
            [180, 'stn', 'Standards body', 'marcrelator'],
            [181, 'ivr', 'Interviewer', 'marcrelator'],
            [182, 'att', 'Attributed name', 'marcrelator'],
            [183, 'prn', 'Production company', 'marcrelator'],
            [184, 'blw', 'Blurb writer', 'marcrelator'],
            [185, 'aud', 'Author of dialog', 'marcrelator'],
            [186, 'elg', 'Electrician', 'marcrelator'],
            [187, 'aus', 'Screenwriter', 'marcrelator'],
            [188, 'adp', 'Adapter', 'marcrelator'],
            [189, 'dpt', 'Depositor', 'marcrelator'],
            [190, 'msd', 'Musical director', 'marcrelator'],
            [191, 'spy', 'Second party', 'marcrelator'],
            [192, 'cpc', 'Copyright claimant', 'marcrelator'],
            [193, 'ctb', 'Contributor', 'marcrelator'],
            [194, 'wat', 'Writer of added text', 'marcrelator'],
            [195, 'lbt', 'Librettist', 'marcrelator'],
            [196, 'pma', 'Permitting agency', 'marcrelator'],
            [197, 'pup', 'Publication place', 'marcrelator'],
            [198, 'dnr', 'Donor', 'marcrelator'],
            [199, 'eng', 'Engineer', 'marcrelator'],
            [200, 'isb', 'Issuing body', 'marcrelator'],
            [201, 'wde', 'Wood engraver', 'marcrelator'],
            [202, 'rsr', 'Restorationist', 'marcrelator'],
            [203, 'coe', 'Contestant-appellee', 'marcrelator'],
            [204, 'cov', 'Cover designer', 'marcrelator'],
            [205, 'brd', 'Broadcaster', 'marcrelator'],
            [206, 'col', 'Collector', 'marcrelator'],
            [207, 'spk', 'Speaker', 'marcrelator'],
            [208, 'rcp', 'Addressee', 'marcrelator'],
            [209, 'ant', 'Bibliographic antecedent', 'marcrelator'],
            [210, 'red', 'Redaktor', 'marcrelator'],
            [211, 'lyr', 'Lyricist', 'marcrelator'],
            [212, 'ill', 'Illustrator', 'marcrelator'],
            [213, 'srv', 'Surveyor', 'marcrelator'],
            [214, 'dto', 'Dedicator', 'marcrelator'],
            [215, 'hnr', 'Honoree', 'marcrelator'],
            [216, 'mfp', 'Manufacture place', 'marcrelator'],
            [217, 'ctt', 'Contestee-appellant', 'marcrelator'],
            [218, 'bnd', 'Binder', 'marcrelator'],
            [219, 'cas', 'Caster', 'marcrelator'],
            [220, 'frg', 'Forger', 'marcrelator'],
            [221, 'pht', 'Photographer', 'marcrelator'],
            [222, 'ctr', 'Contractor', 'marcrelator'],
            [223, 'apl', 'Appellant', 'marcrelator'],
            [224, 'wit', 'Witness', 'marcrelator'],
            [225, 'dfd', 'Defendant', 'marcrelator'],
            [226, 'dte', 'Dedicatee', 'marcrelator'],
            [227, 'lel', 'Libelee', 'marcrelator'],
            [228, 'pth', 'Patent holder', 'marcrelator'],
            [229, 'edt', 'Editor', 'marcrelator'],
            [230, 'acp', 'Art copyist', 'marcrelator'],
            [231, 'jug', 'Jurisdiction governed', 'marcrelator'],
            [232, 'rce', 'Recording engineer', 'marcrelator'],
            [233, 'prg', 'Programmer', 'marcrelator'],
            [234, 'fld', 'Field director', 'marcrelator'],
            [235, 'rpy', 'Responsible party', 'marcrelator'],
            [236, 'vdg', 'Videographer', 'marcrelator'],
            [237, 'dgs', 'Degree supervisor', 'marcrelator'],
            [238, 'rpc', 'Radio producer', 'marcrelator'],
            [239, 'dpc', 'Depicted', 'marcrelator'],
            [240, 'cns', 'Censor', 'marcrelator'],
            [241, 'wal', 'Writer of added lyrics', 'marcrelator'],
            [242, 'cpl', 'Complainant', 'marcrelator'],
            [243, 'nrt', 'Narrator', 'marcrelator'],
            [244, 'bsl', 'Bookseller', 'marcrelator'],
            [245, 'rsg', 'Restager', 'marcrelator'],
            [246, 'mrk', 'Markup editor', 'marcrelator'],
            [247, 'anm', 'Animator', 'marcrelator'],
            [248, 'dgg', 'Degree granting institution', 'marcrelator'],
            [249, 'mus', 'Musician', 'marcrelator'],
            [250, 'arc', 'Architect', 'marcrelator'],
            [251, 'osp', 'Onscreen presenter', 'marcrelator'],
            [252, 'trc', 'Transcriber', 'marcrelator'],
            [253, 'bpd', 'Bookplate designer', 'marcrelator'],
            [254, 'bdd', 'Binding designer', 'marcrelator'],
            [255, 'edc', 'Editor of compilation', 'marcrelator'],
            [256, 'lil', 'Libelant', 'marcrelator'],
            [257, 'rst', 'Respondent-appellant', 'marcrelator'],
            [258, 'cmt', 'Compositor', 'marcrelator'],
            [259, 'clr', 'Colorist', 'marcrelator'],
            [260, 'cng', 'Cinematographer', 'marcrelator'],
            [261, 'aut', 'Author', 'marcrelator'],
            [262, 'clt', 'Collotyper', 'marcrelator'],
            [263, 'opn', 'Opponent', 'marcrelator'],
            [264, 'ann', 'Annotator', 'marcrelator'],
            [265, 'cou', 'Court governed', 'marcrelator'],
            [266, 'act', 'Actor', 'marcrelator'],
            [267, 'std', 'Set designer', 'marcrelator'],
            [268, 'ilu', 'Illuminator', 'marcrelator']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb']
        rows = [[d['id'], json.dumps(d)] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_classification_type(m):
    tbl = 'classification_type'
    cols = ['id','name']
    rows = [[1, 'Library of Congress Classification'],
            [2, 'Dewey Decimal Classification'],
            [3, 'Government Document Classification'],
            [4, 'Library of Congress Classification (Local)']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_instance_type(m):
    tbl = 'instance_type'
    cols = ['id', 'code', 'name', 'source']
    rows = [[1, 'tdf', 'three-dimensional form', 'rdacontent'],
             [2, 'sti', 'still image ', 'rdacontent'],
             [3, 'tci', 'tactile image ', 'rdacontent'],
             [4, 'tdm', 'three-dimensional moving image', 'rdacontent'],
             [5, 'prm', 'performed music ', 'rdacontent'],
             [6, 'cod', 'computer dataset', 'rdacontent'],
             [7, 'cri', 'cartographic image', 'rdacontent'],
             [8, 'snd', 'sounds', 'rdacontent'],
             [9, 'tdi', 'two-dimensional moving image', 'rdacontent'],
             [10, 'txt', 'text', 'rdacontent'],
             [11, 'cop', 'computer program', 'rdacontent'],
             [12, 'zzz', 'unspecified ', 'rdacontent'],
             [13, 'tcm', 'tactile notated music ', 'rdacontent'],
             [14, 'tcf', 'tactile three-dimensional form', 'rdacontent'],
             [15, 'tcn', 'tactile notated movement', 'rdacontent'],
             [16, 'crn', 'cartographic tactile three-dimensional form ', 'rdacontent'],
             [17, 'crm', 'cartographic moving image ', 'rdacontent'],
             [18, 'xxx', 'other ', 'rdacontent'],
             [19, 'tct', 'tactile text', 'rdacontent'],
             [20, 'ntv', 'notated movement', 'rdacontent'],
             [21, 'crt', 'cartographic tactile image', 'rdacontent'],
             [22, 'crd', 'cartographic dataset', 'rdacontent'],
             [23, 'crf', 'cartographic three-dimensional form ', 'rdacontent'],
             [24, 'ntm', 'notated music ', 'rdacontent'],
             [25, 'spw', 'spoken word ', 'rdacontent']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb']
        rows = [[d['id'], json.dumps(d)] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_instance_format(m):
    tbl = 'instance_format'
    cols = ['id', 'code', 'name', 'source']
    rows = [[1, 'nz', 'unmediated -- other', 'rdacarrier'],
             [2, 'vf', 'video -- videocassette', 'rdacarrier'],
             [3, 'gc', 'projected image -- filmstrip cartridge', 'rdacarrier'],
             [4, 'nn', 'unmediated -- flipchart', 'rdacarrier'],
             [5, 'no', 'unmediated -- card', 'rdacarrier'],
             [6, 'sw', 'audio -- audio wire reel', 'rdacarrier'],
             [7, 'mr', 'projected image -- film reel', 'rdacarrier'],
             [8, 'cz', 'computer -- other', 'rdacarrier'],
             [9, 'hd', 'microform -- microfilm reel', 'rdacarrier'],
             [10, 'he', 'microform -- microfiche', 'rdacarrier'],
             [11, 'ss', 'audio -- audiocassette', 'rdacarrier'],
             [12, 'es', 'stereographic -- stereograph disc', 'rdacarrier'],
             [13, 'hj', 'microform -- microfilm roll', 'rdacarrier'],
             [14, 'ha', 'microform -- aperture card', 'rdacarrier'],
             [15, 'gt', 'projected image -- overhead transparency', 'rdacarrier'],
             [16, 'sd', 'audio -- audio disc', 'rdacarrier'],
             [17, 'ce', 'computer -- computer disc cartridge', 'rdacarrier'],
             [18, 'gs', 'projected image -- slide', 'rdacarrier'],
             [19, 'sq', 'audio -- audio roll', 'rdacarrier'],
             [20, 'vc', 'video -- video cartridge', 'rdacarrier'],
             [21, 'na', 'unmediated -- roll', 'rdacarrier'],
             [22, 'mf', 'projected image -- film cassette', 'rdacarrier'],
             [23, 'si', 'audio -- sound track reel', 'rdacarrier'],
             [24, 'hb', 'microform -- microfilm cartridge', 'rdacarrier'],
             [25, 'pp', 'microscopic -- microscope slide', 'rdacarrier'],
             [26, 'sg', 'audio -- audio cartridge', 'rdacarrier'],
             [27, 'hz', 'microform -- other', 'rdacarrier'],
             [28, 'nc', 'unmediated -- volume', 'rdacarrier'],
             [29, 'mc', 'projected image -- film cartridge', 'rdacarrier'],
             [30, 'st', 'audio -- audiotape reel', 'rdacarrier'],
             [31, 'mz', 'projected image -- other', 'rdacarrier'],
             [32, 'cf', 'computer -- computer tape cassette', 'rdacarrier'],
             [33, 'se', 'audio -- audio cylinder', 'rdacarrier'],
             [34, 'ck', 'computer -- computer card', 'rdacarrier'],
             [35, 'hg', 'microform -- microopaque', 'rdacarrier'],
             [36, 'mo', 'projected image -- film roll', 'rdacarrier'],
             [37, 'ez', 'stereographic -- other', 'rdacarrier'],
             [38, 'vd', 'video -- videodisc', 'rdacarrier'],
             [39, 'vr', 'video -- videotape reel', 'rdacarrier'],
             [40, 'ca', 'computer -- computer tape cartridge', 'rdacarrier'],
             [41, 'eh', 'stereographic -- stereograph card', 'rdacarrier'],
             [42, 'vz', 'video -- other', 'rdacarrier'],
             [43, 'gd', 'projected image -- filmslip', 'rdacarrier'],
             [44, 'hh', 'microform -- microfilm slip', 'rdacarrier'],
             [45, 'zu', 'unspecified -- unspecified', 'rdacarrier'],
             [46, 'sz', 'audio -- other', 'rdacarrier'],
             [47, 'cr', 'computer -- online resource', 'rdacarrier'],
             [48, 'cb', 'computer -- computer chip cartridge', 'rdacarrier'],
             [49, 'gf', 'projected image -- filmstrip', 'rdacarrier'],
             [50, 'hf', 'microform -- microfiche cassette', 'rdacarrier'],
             [51, 'pz', 'microscopic -- other', 'rdacarrier'],
             [52, 'nb', 'unmediated -- sheet', 'rdacarrier'],
             [53, 'ch', 'computer -- computer tape reel', 'rdacarrier'],
             [54, 'nr', 'unmediated -- object', 'rdacarrier'],
             [55, 'cd', 'computer -- computer disc', 'rdacarrier'],
             [56, 'hc', 'microform -- microfilm cassette', 'rdacarrier']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb']
        rows = [[d['id'], json.dumps(d)] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_mode_of_issuance(m):
    tbl = 'mode_of_issuance'
    cols = ['id','name']
    rows = [[1, 'Monographic component part'],
            [2, 'Serial component part'],
            [3, 'Collection'],
            [4, 'Subunit'],
            [5, 'Integrating resource'],
            [6, 'Monograph/Item'],
            [7, 'Serial']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_instance_status(m):
    tbl = 'instance_status'
    cols = ['id', 'code', 'name', 'source']
    rows = [[1, 'uc', 'Uncataloged', 'UC'],
            [2, 'cat', 'Cataloging complete', 'UC'],
            [3, 'sc', 'Short cataloged', 'UC'],
            [4, 'short', 'Temporary category', 'UC'],
            [5, 'online', 'Access level records for Internet resources', 'UC'],
            [6, 'etitle', 'Electronic resource', 'UC'],
            [7, 'noexpo', 'Batch record load no export permited', 'UC'],
            [8, 'toc', 'Table of Contents from BNA', 'UC'],
            [9, 'ddadisc', 'DDA e-title discovery record', 'UC'],
            [10, 'cjk', 'East Asia Cataloging complete', 'UC'],
            [11, 'cjkreco', 'East Asia recon records', 'UC'],
            [12, 'dllreco', 'TALX DLL recon', 'UC'],
            [13, 'recon', 'OCLC Retrocon records', 'UC'],
            [14, 'uf', 'User fast-added', 'UC'],
            [15, 'mcs', 'Mono class sep set record', 'UC'],
            [16, 'xxx', 'xxx - SYSTEMSONLY', 'UC'],
            [17, 'etemp', 'Electronic resource temporary', 'UC'],
            [18, 'oclccm', 'OCLC Collection Manager', 'UC'],
            [19, 'circ', 'Circulation', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')



def load_call_number_type(m):
    tbl = 'call_number_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'UDC', 'folio'],
            [2, 'Superintendent of Documents classification', 'folio'],
            [3, 'Dewey Decimal classification', 'folio'],
            [4, 'Library of Congress classification', 'folio'],
            [5, 'National Library of Medicine classification', 'folio'],
            [6, 'LC Modified', 'folio'],
            [7, 'Shelved separately', 'folio'],
            [8, 'Title', 'folio'],
            [9, 'Shelving control number', 'folio'],
            [10, 'Other scheme', 'folio'],
            [11, 'MOYS', 'folio'],
            [12, 'Source specified in subfield $2', 'folio']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_holdings_type(m):
    tbl = 'holdings_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'physical', 'UC'],
            [2, 'electronic', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_material_type(m):
    tbl = 'material_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'microform', 'UC'],
            [2, 'electronic resource', 'UC'],
            [3, 'text', 'UC'],
            [4, 'unspecified', 'UC'],
            [5, 'dvd', 'UC'],
            [6, 'sound recording', 'UC'],
            [7, 'video recording', 'UC'],
            [8, 'book', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


@timing
def load_loan_types(m):
    tbl = 'loan_type'
    cols = ['id', 'name']
    rows = [[1, 'stks'], [2, 'buo'], [3, 'mus'], [4, 'res2'], [5, 'spcl'], [6, '16mmcam'],
            [7, 'AVSadpt'], [8, 'AVSasis'], [9, 'AVSmic'], [10, 'AVSport'], [11, 'AVSproj'],
            [12, 'AVSscre'], [13, 'batt'], [14, 'boompol'], [15, 'bordirc'], [16, 'cabl'],
            [17, 'camrig'], [18, 'dslr'], [19, 'dslracc'], [20, 'eres'], [21, 'games'],
            [22, 'gaming'], [23, 'grip'], [24, 'hdcam'], [25, 'headph'], [26, 'ilok'],
            [27, 'inhouse'], [28, 'its2adp'], [29, 'its8adp'], [30, 'its8cnf'], [31, 'its8ipd'],
            [32, 'its8lap'], [33, 'lghtmtr'], [34, 'lights'], [35, 'lmc6wk'], [36, 'macadpt'],
            [37, 'mics'], [38, 'micstnd'], [39, 'mixer'], [40, 'monitrs'], [41, 'noncirc'],
            [42, 'online'], [43, 'playbck'], [44, 'projctr'], [45, 'res168'], [46, 'res24'],
            [47, 'res4'], [48, 'res48'], [49, 'res72'], [50, 'sndacc'], [51, 'sndrec'],
            [52, 'spkrs'], [53, 'stilcam'], [54, 'stks14'], [55, 'stks7'], [56, 'tripod'],
            [57, 'vidcam'], [58, 'yerk'], [59, 'illbuo'], [60, 'ill7day'], [61, 'ill7dayrenew'],
            [62, 'ill21day'], [63, 'ill21dayrenew'], [64, 'ill42day'], [65, 'ill42dayrenew'],
            [66, 'ill77day'], [67, 'ill77dayrenew'], [68, 'itspol2adp'], [69, 'itspol8adp'],
            [70, 'itspol8vid'], [71, 'itspol8ipd'], [72, 'itspol8lap']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')

        
def load_electronic_access_relationship(m):
    tbl = 'electronic_access_relationship'
    cols = ['id', 'name', 'source', 'UC']
    rows = [[1, 'No information provided', 'UC'],
            [2, 'Resource', 'UC'],
            [3, 'Version of resource', 'UC'],
            [4, 'Related resource', 'UC'],
            [5, 'No display constant generated', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_statistical_code_type(m):
    # Used by statistical_code
    tbl = 'statistical_code_type'
    cols = ['id','name', 'source']
    rows = [[1, 'University of Chicago', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['id'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['id'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')

        
def load_statistical_code(m):
    # From ole_cat_stat_srch_cd_t
    # Used by holdings, e-holdings, and item
    # Depends on statistical_code_type
    tbl = 'statistical_code'
    cols = ['id', 'code', 'name', 'source', 'statisticalCodeTypeId']
    rows = [[1, 'books', 'Books, print (books)', 'UC', 1],
            [2, 'ebooks', 'Books, electronic (ebooks)', 'UC', 1],
            [3, 'music', 'Music scores, print (music)', 'UC', 1],
            [4, 'emusic', 'Music scores, electronic (emusic)', 'UC', 1],
            [5, 'rmusic', 'Music sound recordings (rmusic)', 'UC', 1],
            [6, 'rnonmusic', 'Non-music sound recordings (rnonmusic)', 'UC', 1],
            [7, 'visual', 'Visual materials, DVDs, etc. (visual)', 'UC', 1],
            [8, 'mmedia', 'Mixed media (mmedia)', 'UC', 1],
            [9, 'compfiles', 'Computer files, CDs, etc. (compfiles)', 'UC', 1],
            [10, 'mfilm', 'Microfilm (mfilm)', 'UC', 1],
            [11, 'mfiche', 'Microfiche (mfiche)', 'UC', 1],
            [12, 'maps', 'Maps, print (maps)', 'UC', 1],
            [13, 'emaps', 'Maps, electronic (emaps)', 'UC', 1],
            [14, 'serials', 'Serials, print (serials)', 'UC', 1],
            [15, 'eserials', 'Serials, electronic (eserials)', 'UC', 1],
            [16, 'arch', 'Archives (arch)', 'UC', 1],
            [17, 'its', 'Information Technology Services (its)', 'UC', 1],
            [18, 'mss', 'Manuscripts (mss)', 'UC', 1],
            [19, 'vidstream', 'Streaming video (vidstream)', 'UC', 1],
            [20, 'audstream', 'Streaming audio (audstream)', 'UC', 1], 
            [21, 'withdrawn', 'Withdrawn (withdrawn)', 'UC', 1],
            [22, 'evisual', 'visual, static, electronic', 'UC', 1],
            [23, 'polsky', 'Polsky TECHB@R(polsky)', 'UC', 1]]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l:
        d['id'] = m.uuid[tbl][d['id']]
        d['statisticalCodeTypeId'] = m.uuid['statistical_code_type'][d['statisticalCodeTypeId']]
        d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by', 'statisticalcodetypeid']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time),
                 m.user_id, d['statisticalCodeTypeId']] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_locinstitution(m):
    tbl = 'locinstitution'
    cols = ['id','name', 'code']
    rows = [[1, 'University of Chicago (UC)', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    # if m.load_ref_tables:
    if True:
        cols = ['_id','jsonb', 'creation_date,', 'created_by']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')

            
def load_loccampus(m):
    tbl = 'loccampus'
    cols = ['id','name', 'code', 'institutionId']
    rows = [[1, 'Hyde Park (HP)', 'HP', 1]]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['institutionId'] = m.uuid['locinstitution'][d['institutionId']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    # if m.load_ref_tables:
    if True:
        cols = ['_id','jsonb', 'creation_date,', 'created_by', 'institutionid']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id, d['institutionId']] \
                for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


def load_loclibraries(m):
    tbl = 'loclibrary'
    cols = ['id','name', 'code', 'campusId']
    rows = [[1, 'Mansueto Library (ASR)', 'ASR', 1],
            [2, 'Regenstein (JRL)', 'JRL', 1],
            [3, 'Crerar (JCL)', 'JCL', 1],
            [4, 'Special Collections (SPCL)', 'SPCL', 1],
            [5, 'IT Services at Regenstein (ITS)', 'ITS', 1],
            [6, 'Online (Online)', 'Online', 1],
            [7, 'University of Chicago (UCX)', 'UCX', 1],
            [8, 'Internet', 'AANet', 1],
            [9, 'Eckhart (Eck)', 'Eck', 1],
            [10, 'Social Service Administration (SSAd)', 'SSAd', 1],
            [11, "D'Angelo Law (DLL)", 'DLL', 1],
            [12, 'MCS (MCS)', 'MCS', 1],
            [13, 'IT Services at Polsky (POLSKY)', 'POLSKY', 1],
            [14, 'Logan Media Center Cage', 'LMC', 1],
            [15, 'Unknown', 'unk', 1]]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['campusId'] = m.uuid['loccampus'][d['campusId']]
    for d in l: d['metadata'] = add_json_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.load_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by', 'campusid']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id, \
                 d['campusId']] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


# def load_locations(m):
#     sql = "select c.locn_cd, c.locn_name, b.locn_cd from ole_locn_t a join ole_locn_t b " \
#           "on a.locn_id = b.parent_locn_id join ole_locn_t c on b.locn_id = c.parent_locn_id " \
#           "where a.level_id = 1;"
#     m.mdb.cur.execute(sql)
#     rows = [r for r in m.mdb.cur.fetchall()]

#     tbl = 'location'
#     cols = ['id','code', 'name', 'libraryId', 'isActive', 'campusId', 'institutionId']
#     lib = {'ASR': 1, 'JRL': 2, 'JCL': 3, 'SPCL': 4, 'ITS': 5, 'Online': 6, 'UCX': 7, 'AANet': 8,
#            'Eck': 9, 'SSAd': 10, 'DLL': 11, 'MCS': 12, 'POLSKY': 13, 'LMC': 14, 'unk': 15}
#     l = [dict(zip(cols, [i] + list(r) + [True, 1, 1])) for i,r in zip(range(1,500),rows)]
#     acc = []
#     for d in l:
#         j = dict([
#             ['id', m.uuid[tbl][d['id']]],
#             ['code', d['code'] + '-' + str(lib[d['libraryId']])],
#             ['description', d['name']],
#             ['discoveryDisplayName', d['name']],
#             ['name', d['code'] + '-' + str(lib[d['libraryId']])],
#             ['libraryId', m.uuid['loclibrary'][lib[d['libraryId']]]],
#             ['campusId', m.uuid['loccampus'][d['campusId']]],
#             ['institutionId', m.uuid['locinstitution'][d['institutionId']]],
#             ['primaryServicePoint', '0f4e68f8-bfdc-4ea1-8ca1-36def8fb9c6a'],
#             ['servicePointIds', '0f4e68f8-bfdc-4ea1-8ca1-36def8fb9c6a'],
#             ['metadata', add_json_metadata(m)]])
#         acc.append(j)
#     m.ref[tbl] = dict([[j['code'], j['id']] for j in acc])
#     m.ref_dict[tbl] = dict([[j['code'], j] for j in acc])
        
#     if m.load_ref_tables:
#         cols = ['_id','jsonb','creation_date,','created_by','institutionid','campusid','libraryid']
#         rows = [[j['id'], json.dumps(j), datetime_to_str(m.cur_time), m.user_id, \
#                  j['institutionId'], j['campusId'], j['libraryId']] for j in acc]
#         execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
#         m.pdb.cur.execute('commit')

        
def load_locations(m):
    sql = "select c.locn_cd, c.locn_name, b.locn_cd from ole_locn_t a join ole_locn_t b " \
          "on a.locn_id = b.parent_locn_id join ole_locn_t c on b.locn_id = c.parent_locn_id " \
          "where a.level_id = 1;"
    m.mdb.cur.execute(sql)
    rows = [r for r in m.mdb.cur.fetchall()]

    sql = "select location from (select distinct location from ole_ds_holdings_t union " \
          "select distinct location from ole_ds_item_t) as loc where location not in " \
          "(select concat(a.locn_cd,'/',b.locn_cd,'/',c.locn_cd) as code  from ole_locn_t a " \
          "join ole_locn_t b on a.locn_id = b.parent_locn_id " \
          "join ole_locn_t c on b.locn_id = c.parent_locn_id where a.level_id = 1)"
    m.mdb.cur.execute(sql)
    l = [r.split('/') for l in m.mdb.cur.fetchall() for r in l if len(r.split('/')) == 3]
    rows.extend([[c,c,b] for a,b,c in l])
    tbl = 'location'
    cols = ['id','code', 'name', 'libraryId', 'isActive', 'campusId', 'institutionId']
    lib = {'ASR': 1, 'JRL': 2, 'JCL': 3, 'SPCL': 4, 'ITS': 5, 'Online': 6, 'UCX': 7, 'AANet': 8,
           'Eck': 9, 'SSAd': 10, 'DLL': 11, 'MCS': 12, 'POLSKY': 13, 'LMC': 14, 'unk': 15}
    l = [dict(zip(cols, [i] + list(r) + [True, 1, 1])) for i,r in zip(range(1,500),rows)]
    
    for d in l:
        d['id'] = m.uuid[tbl][d['id']]
        d['code'] = d['code'] + '-' + str(lib[d['libraryId']])
        d['description'] = d['name']
        d['discoveryDisplayName'] = d['name']
        d['name'] = d['code']
        d['libraryId'] = m.uuid['loclibrary'][lib[d['libraryId']]]
        d['campusId'] = m.uuid['loccampus'][d['campusId']]
        d['institutionId'] = m.uuid['locinstitution'][d['institutionId']]
        d['primaryServicePoint'] = '0f4e68f8-bfdc-4ea1-8ca1-36def8fb9c6a'
        d['servicePointIds'] = ['0f4e68f8-bfdc-4ea1-8ca1-36def8fb9c6a']
        d['metadata'] = add_json_metadata(m)
        m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
        m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
        
    if m.load_ref_tables:
        cols = ['_id','jsonb','creation_date,','created_by','institutionid','campusid','libraryid']
        rows = [[d['id'], json.dumps(d), datetime_to_str(m.cur_time), m.user_id, \
                 d['institutionId'], d['campusId'], d['libraryId']] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')

        
@timing
def load_service_points(m):
    tbl = 'service_point'
    cols = ['id', 'code', 'name', 'shelvingLagTime', 'pickupLocation', 'locationIds']
    rows = [[1, 'API', 'API', 15, True, 11],
            [2, 'SSAd', 'Social Service Administration Library', 1440, True, 12],
            [3, 'ECKHART', 'Eckhart Library', 1440, True, 13],
            [4, 'MANSUETO', 'Mansueto Library', 1440, True, 14],
            [5, 'ITS', 'TECHB@R', 1440, False, 15],
            [6, 'RECORDINGS', 'Recordings Collection', 1440, False, 16],
            [7, 'SCRC', 'Special Collections Research Center', 1440, False, 17],
            [8, 'POLSKY', 'Polsky TECHB@R', 1440, False, 18],
            [9, 'JRLMAIN', 'Regenstein Library, 1st Floor', 1440, True, 7],
            [10, 'LAW', 'D Angelo Law Library', 1440, True, 8],
            [11, 'CRERAR', 'Crerar Library', 1440, True, 9]]
    l = [dict(zip(cols, r)) for r in rows]
    acc = []
    for d in l:
        j = dict([['id', m.uuid[tbl][d['id']]],
                  ['name', d['code']],
                  ['code', d['code']],
                  ['discoveryDisplayName', d['name']],
                  ['description', d['name']],
                  ['shelvingLagTime', d['shelvingLagTime']],
                  ['pickupLocation', d['pickupLocation']],
                  ['holdShelfExpiryPeriod', None],
                  ['staffSlips', []],
                  ['metadata', add_json_metadata(m)]])
        acc.append(j)
    m.ref[tbl] = dict([[j['code'], j['id']] for j in acc])
    m.ref_dict[tbl] = dict([[j['code'], j] for j in acc])
    if m.load_ref_tables:
        rows = [[j['id'], json.dumps(j), datetime_to_str(m.cur_time), m.user_id] for j in acc]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.cur.execute('commit')


##### misc

            
@timing
def exclude_locations(m):
    # temporarily filter out problem holdings using this sql:
    # select distinct location from ole_ds_holdings_t h
    # where location not in 
    # (select concat(c.locn_cd, '/', l.locn_cd, '/', s.locn_cd) from ole_locn_t c
    # left join ole_locn_t l on c.locn_id = l.parent_locn_id
    # left join ole_locn_t s on l.locn_id = s.parent_locn_id
    # where c.level_id = 1 and s.row_act_ind = 'Y');
    return ['Online', 'UC', 'UC/ASR/InProc', 'UC/ASR/Mss', 'UC/ASR/MssCr', 'UC/ASR/Rare',
            'UC/DLL/BorDirc', 'UC/DLL/Gen', 'UC/DLL/LawASR', 'UC/DLL/LawSupr', 'UC/Eck/Order',
            'UC/JCL/JzMon', 'UC/JCL/Mic', 'UC/JCL/Order', 'UC/JCL/Rare', 'UC/JCL/SerCat',
            'UC/JCL/Staff', 'UC/JRL/Arch', 'UC/JRL/ArcMon', 'UC/JRL/ArcRef1', 'UC/JRL/ArcSer',
            'UC/JRL/Drama', 'UC/JRL/EB', 'UC/JRL/Intrnet', 'UC/JRL/JzMon', 'UC/JRL/MoPoRa',
            'UC/JRL/Order', 'UC/JRL/Rare', 'UC/JRL/RareCr', 'UC/JRL/Sci', 'UC/JRL/SSAd',
            'UC/JRL/Staff', 'UC/JRL/unk', 'UC/MapCl', 'UC/MCS/Order', 'UC/MCS/SSAd',
            'UC/SPCL/Gen', 'UC/SPCL/InProc', 'UC/SPCL/Mic', 'UC/SPCL/Order', 'UC/SSAd/ERes',
            'UC/SSAd/InProc', 'UC/SSAd/Order', 'UC/UCX/Rare', 'UC/unk/unk', 'UCX/InProc',
            'UC/AANet/Intrnet', 'UC/LMC/LMCstaf', 'UC/LMC/LMCexib', 'UC/JRL/Art420',
            'UC/JRL/Rec', 'UC/JRL/CircPer', 'UC/LMC/LMCgear', 'UC/LMC/LMCcabl',
            'UC/SSAd/SSAdPam', 'UC/LMC/LMCacc', 'UC/JCL/SRefPer', 'UC/JRL/Res', 'UC/JRL/RecHP',
            'UC/JCL/SciTecP', 'UC/UCX/unk']

    
@timing
def fetch_col_names(m):
    m.col = dict()
    sql = "select table_name, column_name  from information_schema.columns;"
    m.mdb.cur.execute(sql)
    lst = [[t.lower(),c.lower()] for t,c in m.mdb.cur.fetchall()]
    for t,c in lst:
        if not t in m.col: m.col[t] = [c]
        else: m.col[t].append(c)
    # for t in [t['tbl'] for t in m.tables if t['db'] == 'ole']: m.col[t].append('uuid')

    sql = "select table_name, column_name  from information_schema.columns"
    m.pdb.cur.execute(sql)
    lst = [[t.lower(),c.lower( )] for t,c in m.pdb.cur.fetchall()]
    for t,c in lst:
        if not t in m.col: m.col[t] = [c]
        else: m.col[t].append(c)


@timing 
def fetch_tag_maps(m):
    with open(m.tag_map_filename) as f:
        rows = [ln.strip().split() for ln in f.readlines()][2:]
    for r in rows:
        tag = r[0]
        rep = True if r[1] == 'y' else False
        req = True if r[1] == 'y' else False
        lst = [] if r[3] == 'None' else (m.alpha_digits if r[3] == '*' else r[3:-1])
        jo_lst = r[-1].split(',')
        for jo in jo_lst: m.jo[jo] = []
        m.map[tag] = dict([['rep', rep], ['req', req], ['lst', lst], ['jo', jo_lst]])
    m.log.debug("fetched tag maps")

    
def get_uuids(m, table_name):
    sql = f"select uuid from cat.uuid where table_name = '{table_name}'"
    m.mdb.cur.execute(sql)
    return [i for r in m.mdb.cur.fetchall() for i in r]


def fetch_uuids(m, table_name):
    sql = f"select id, uuid from cat.uuid where table_name = '{table_name}'"
    m.mdb.cur.execute(sql)
    return m.mdb.cur.fetchall()


def local_to_utc(date):
    if isinstance(date, datetime.datetime):
        local = pytz.timezone("America/Chicago")
        local_dt = local.localize(date, is_dst=True)
        utc_dt = local_dt.astimezone(pytz.utc)
        return utc_dt
    else: return date


def utc_to_local(date):
    if isinstance(date, datetime.datetime):
        local = pytz.timezone("America/Chicago")
        utc = pytz.utc
        utc_dt = utc.localize(date, is_dst=True)
        local_dt = utc_dt.astimezone(local)
    else: return local_dt


def datetime_to_str(date):
    if isinstance(date, datetime.datetime):
        return date.strftime("%Y-%m-%dT%H:%M:%S")
    else: return date


def str_to_datetime(date):
    if isinstance(date, str):
        date = date.split('.')[0]
        return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
    else: return date


def serialize_datatype(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        type_name = obj.__class__.__name__
        raise TypeError("Object of type '%s' is not JSON serializable" % type_name)


def create_batches(m, rows):
    return [rows[i:i+m.batch_size] for i in range(0,len(rows),m.batch_size)]


def truncate_rows(m):
    lst = ['item', 'holdings_record', 'instance']
    for table in lst:
        m.pdb.cur.execute('truncate table %s cascade;' % table)
        m.pdb.cur.execute("commit")
    

class mdb_class:
    def __init__(self, m):
        self.config = {'host':m.ole_host,
                       'database':'ole'}
        self.con = mysql.connector.connect(
            option_files=['/home/arnt/.my.cnf'],
            option_groups=['client'],
            **self.config)
        self.cur = self.con.cursor()
        self.cur.execute("set session sql_mode = ''")


class pdb_class:
    schemas = ['diku_mod_calendar', 'diku_mod_circulation_storage',
               'diku_mod_configuration', 'diku_mod_inventory_storage',
               'diku_mod_login', 'diku_mod_notes', 'diku_mod_notify',
               'diku_mod_permissions', 'diku_mod_users', 'diku_mod_vendors', 'public']
    def __init__(self, m):
        self.dsn = "host='%s' dbname='folio' user='folio'" % m.folio_host
        self.con = psycopg2.connect(self.dsn)
        self.cur = self.con.cursor()
        self.cur.execute("set search_path to %s;" % ','.join(pdb_class.schemas))


def initialize_logging(m):
    d = dict([['debug',logging.DEBUG],['info',logging.INFO],['warning',logging.WARN],
              ['error',logging.ERROR],['critical',logging.CRITICAL]])
    logging.basicConfig(level=d[m.log_level],
                        format = '%(asctime)s  %(name)-7s %(levelname)-6s %(message)s',
                        datefmt = '%m-%d %H:%M',
                        filename = m.log_file,
                        filemode = 'w')
    m.log = logging.getLogger('loader')
    console = logging.StreamHandler()
    console.setLevel(d[m.log_level])
    formatter = logging.Formatter('%(name)-6s:  %(levelname)-5s %(message)s')
    console.setFormatter(formatter)
    if not m.log.handlers: m.log.addHandler(console)


def open_reject_file(m):
    m.reject_file = open(m.reject_filename, 'w')

        
def close_reject_file(m):
    m.reject_file.close()


def calc_time(m, s, btime):
    etime = time()
    tm = etime - btime
    print('%s: %02d:%02d:%02d' % (s, tm/3600, tm%3600/60, tm%60))


def fetch_rows(m, table_name, key):
    cols = m.col[table_name]
    m.mdb.cur.execute("select * from %s" % table_name)
    lst = [dict([[a,b] for a,b in zip(cols,row)]) for row in m.mdb.cur.fetchall()]
    return dict([[d['key'],d] for d in lst])
    

def update_ole_locn_t(m):
    sql = "alter table ole_locn_t add location varchar(50) null;"
    m.mdb.cur.execute(sql)
    m.mdb.cur.execute("commit")
    sql = "alter table ole_locn_t add index location(location);"
    m.mdb.cur.execute(sql)
    m.mdb.cur.execute("commit")
    sql = "update ole_locn_t s " \
          "left join ole_locn_t l on l.locn_id = s.parent_locn_id " \
          "left join ole_locn_t i on i.locn_id = l.parent_locn_id " \
          "set s.location = concat(i.locn_cd, '/', l.locn_cd, '/', s.locn_cd) " \
          "where i.level_id = 1"
    m.mdb.cur.execute(sql)
    m.mdb.cur.execute("commit")

    
@timing
def drop_constraints(m):
    with open(m.drop_constraints_filename) as f: lst = f.readlines()
    for sql in lst: 
        try:
            m.pdb.cur.execute(sql)        
            m.pdb.cur.execute("commit")        
        except:
            m.log.error('unable to drop constraint:\n%s' \
                        % sql ,exc_info=m.stack_trace)
            continue

    
@timing
def add_constraints(m):
    with open(m.add_constraints_filename) as f: lst = f.readlines()
    for sql in lst: 
        try:
            m.pdb.cur.execute(sql)        
            m.pdb.cur.execute("commit")        
        except:
            m.log.error('unable to create constraint:\n%s' \
                        % sql ,exc_info=m.stack_trace)
            continue


def diff_indexes(m):
    with open('drop_indexes.sql') as f: new_lst = f.readlines()
    with open('drop_indexes.sql.elderberry') as f: old_lst = f.readlines()
    print(set(old_lst) ^ set(new_lst))

    
@timing
def drop_indexes(m):
    with open(m.drop_indexes_filename) as f: lst = f.readlines()
    for sql in lst: 
        try:
            m.pdb.cur.execute(sql)        
            m.pdb.cur.execute("commit")        
        except:
            m.log.error('unable to drop index:\n%s' % sql ,exc_info=m.stack_trace)
            continue

    
@timing
def add_indexes(m):
    with open(m.add_indexes_filename) as f: lst = f.readlines()
    for sql in lst: 
        try:
            m.pdb.cur.execute(sql)        
            m.pdb.cur.execute("commit")        
        except:
            m.log.error('unable to create index:\n%s' % sql ,exc_info=m.stack_trace)
            continue


##### uuids        

def create_uuids(m):
    btime = time()
    get_tables_needing_uuids(m)
    create_uuid_table(m)
    assign_uuids(m)
    create_indexes(m)
    calc_time(m, "Time taken to create uuids", btime)

        
def create_uuid_table(m):
    if m.create_uuid_table:
        btime = time()
        sql = "drop table if exists cat.uuid"
        m.mdb.cur.execute(sql)
        sql = """
        create table cat.uuid(
        auto_id int not null auto_increment,
        id int not null,
        uuid varchar(36) not null,
        table_name varchar(128) not null,
        column_name varchar(128) not null,
        primary key(auto_id))
        row_format = dynamic;"""
        m.mdb.cur.execute(sql)
        calc_time(m, "Time taken to create uuid table", btime)


def get_tables_needing_uuids(m):
    # assign the number of uuids to be created for folio tables
    l = [
        {'db':'ole','tbl':'ole_ds_bib_t','col':'bib_id','count':None},
        {'db':'ole','tbl':'ole_ds_holdings_t','col':'holdings_id','count':None},
        {'db':'ole','tbl':'ole_ds_item_t','col':'item_id','count':None},
        {'db':'ole','tbl':'ole_circ_dsk_dtl_t','col':'crcl_dsk_dtl_id','count':None},
        {'db':'ole','tbl':'ole_crcl_dsk_locn_t','col':'ole_crcl_dsk_locn_id','count':None},
        {'db':'ole','tbl':'ole_crcl_dsk_t','col':'ole_crcl_dsk_id','count':None},
        # {'db':'ole','tbl':'ole_locn_t','col':'locn_id','count':None},
        {'db':'ole','tbl':'ole_cat_bib_record_stat_t','col':'bib_record_stat_id','count':None},
        {'db':'folio','tbl':'alternative_title_type','col':'id','count':3},
        {'db':'folio','tbl':'identifier_type','col':'id','count':12},
        {'db':'folio','tbl':'contributor_name_type','col':'id','count':3},
        {'db':'folio','tbl':'contributor_type','col':'id','count':268},
        {'db':'folio','tbl':'classification_type','col':'id','count':4},
        {'db':'folio','tbl':'instance_type','col':'id','count':25},
        {'db':'folio','tbl':'instance_format','col':'id','count':56},
        {'db':'folio','tbl':'mode_of_issuance','col':'id','count':7},
        {'db':'folio','tbl':'instance_status','col':'id','count':19},
        {'db':'folio','tbl':'call_number_type','col':'id','count':12},
        {'db':'folio','tbl':'holdings_type','col':'id','count':2},
        {'db':'folio','tbl':'material_type','col':'id','count':8},
        {'db':'folio','tbl':'electronic_access_relationship','col':'id','count':5},
        {'db':'folio','tbl':'statistical_code_type','col':'id','count':1},
        {'db':'folio','tbl':'statistical_code','col':'id','count':23},
        {'db':'folio','tbl':'instance_source_marc','col':'id','count':1},
        {'db':'folio','tbl':'ext_ownership_type','col':'id','count':3},
        {'db':'folio','tbl':'locinstitution','col':'id','count':1},
        {'db':'folio','tbl':'loccampus','col':'id','count':1},
        {'db':'folio','tbl':'loclibrary','col':'id','count':15},
        {'db':'folio','tbl':'location','col':'id','count':227},
        {'db':'folio','tbl':'service_point','col':'id','count':11},
        {'db':'folio','tbl':'loan_type','col':'id','count':72}
    ]
    for d in l: d['add'] = []
    for d in l: d['del'] = []
    m.tables = l
    

def assign_uuids(m):
    for d in m.tables:
        btime = time()
        if d['db'] == 'ole':
            sql = "select %s from %s" % (d['col'],d['tbl'])
            m.mdb.cur.execute(sql)
            ole_ids = set([int(i) for r in m.mdb.cur.fetchall() for i in r])
            sql = "select id from cat.uuid where table_name = '%s'" % d['tbl']
            m.mdb.cur.execute(sql)
            cat_ids = set([i for r in m.mdb.cur.fetchall() for i in r])
            d['add'] = sorted(list(ole_ids - cat_ids))
            d['del'] = sorted(list(cat_ids - ole_ids))
        elif d['db'] == 'folio':
            sql = "select id from cat.uuid where table_name = '%s'" % d['tbl']
            m.mdb.cur.execute(sql)
            old_ids = set([i for r in m.mdb.cur.fetchall() for i in r])
            new_ids = set(list(range(1,d['count']+1)))
            d['del'] = sorted(list(old_ids - new_ids))
            d['add'] = sorted(list(new_ids - old_ids))
            calc_time(m, "Time taken to fetch ids for %s" % d['tbl'], btime)
    delete_uuids(m)
    add_uuids(m)

    
def delete_uuids(m):
    for d in m.tables:
        btime = time()
        for b in create_batches(m, d['del']):
            s = ','.join(['%s'] * len(b))
            sql = "delete from cat.uuid where table_name = '%s' and id in (%s)" % (d['tbl'], s)
            m.mdb.cur.execute(sql,b)
            m.mdb.cur.execute('commit')
        print("Number of uuids deleted for %s: %d" % (d['tbl'],len(d['del'])))
        calc_time(m, "Time taken to delete rows", btime)
            
    
def add_uuids(m):
    sql = "insert into cat.uuid (id, uuid, table_name, column_name) values (%s, %s, %s, %s);"
    for d in m.tables:
        btime = time()
        for b in create_batches(m, d['add']):
            rows = [[id, str(uuid4()), d['tbl'], d['col']] for id in b]
            m.mdb.cur.executemany(sql,rows)
            m.mdb.cur.execute('commit')
        print("Number of uuids added for %s: %d" % (d['tbl'],len(d['add'])))
        calc_time(m, "Time taken to add uuids", btime)
    

def create_indexes(m):
    if m.create_uuid_table:
        btime = time()
        for col in ['id', 'uuid', 'table_name', 'column_name']:
            sql = "create index %s using btree on cat.uuid(%s);" % (col,col)
            m.mdb.cur.execute(sql)
        calc_time(m, "Time taken to create uuid indexes", btime)


@timing
def validate(m):
    m.schema_dir = None
    with open('/home/arnt/dev/folio/import/inventory/latest/instance.json') as f:
        schema = json.loads(f.read())
    try:
        jsonschema.validate(m.ins_row['jsonb'], schema)
    except:
        m.log.error("Validation error", exc_info=m.stack_trace)
        m.reject_file.write('%s\t%s\t%s\n' % (m.ins_row['bib_id'], None, None))
        m.log.error('Unable to validate json record: %d' % m.ins_row['bib_id'],exc_info=m.stack_trace)
        m.log.info(pformat(m.ins_row))
        if m.ins_row['bib_id'] in m.cur_batch: m.cur_batch.remove(m.ins_row['bib_id'])

        
def assert_valid_schema(data, schema_file):
    global schema
    """ Checks whether the given data matches the schema """

    schema = _load_json_schema(schema_file)
    return validate(data, schema)


def _load_json_schema(filename):
    """ Loads the given schema file """

    relative_path = join('inventory/latest', filename)
    absolute_path = abspath(relative_path)
    
    base_path = dirname(absolute_path)
    base_uri = 'file://{}/'.format(base_path)

    with open(absolute_path) as schema_file:
        return jsonref.loads(schema_file.read(), base_uri=base_uri, jsonschema=True)



def fetch_json_schemas(m):
    clone_path = '../temp/'
    d = {'inventory': {'repo': 'mod_inventory_storage', 'repo_path': 'ramls',
                       'local_path': 'schemas', 'glob': '*.schema'},
         'raml-util': {'repo': 'raml', 'repo_path': 'schemas', 'local_path': 'schemas/raml-util',
                       'glob': '*.json'}}
    for k in d.keys():
        cmd = f"git clone https://github.com/folio-org/{d[k]['repo']} {clone_path + d[k]['repo']}"
        run(cmd.split(), Check=True)

        

        
def fetch_reference_schema_file_names(m):
    l = [f for f in os.listdir(m.ref_schema_dir) if f[-4:] == 'json' and f[-6:] != 's.json']
    l = [f for f in l if f not in ['instance.json', 'holdingsrecord.json', 'item.json']]
    m.ref_schema_names = l


def fetch_reference_schemas(m):
    m.ref_schema_dict = dict()
    for n in m.ref_schema_names:
        t = n[:-5]
        with open(m.ref_schema_dir + n) as f:
            j = json.loads(f.read())
            m.ref_schema_dict[t] = j


# if __name__ == '__main__':
#     m = main()
