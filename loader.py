#!/usr/bin/python3
import sys
from time import *
import datetime
from traceback import *
from collections import OrderedDict as dict
import argparse
import mysql.connector
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
from os.path import join, dirname, abspath
import jsonschema
import jsonref
import pendulum
from pathlib import Path
import pathlib
from subprocess import run
import shutil
import fastjsonschema
from functools import reduce


sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'huckleberry', '-v', '-l', '596805']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'huckleberry', '-v', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'chokecherry', '-v', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'chokecherry', '-v', '-l', '596805']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'chokecherry', '-v', '-l', '8607961']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-l', '8607961']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-f', 'bib-debug.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-l', '2801553', '2813206', '29301']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-f', 'bib-test.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-l', '1300628', '10790590', '4210619']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-l', '10039608']
# sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-l', '19221']
# sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-u', '-l', '12833']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-f', 'bib-test.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-f', 'bib-prob.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-l', '1300628', '10790590', '4210619']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-l', '1300628']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-l', '4210619']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-l', '1300628', '10790590', '4210619']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-f', 'bib-christie.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-l', '596805']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-f', 'bib-count-10000.txt']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-l', '596805', '1205964', '6419471', '10921275', '11099079']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-l', '596805', '1205964', '6419471', '10921275']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-p', '-v', '-l', '596805', '1205964', '5923366']
#sys.argv = ['loader.py', '-a', 'raspberry', '-b', 'lingonberry', '-v', '-f', 'bib-christie.txt']


def get_args():
    """FIXME! briefly describe function

    :returns: 
    :rtype: 

    """
    p = argparse.ArgumentParser()
    p.add_argument('-a', '--ole_host', action='store',
                   required=True, help='Ole host')
    p.add_argument('-b', '--folio_host', action='store',
                   required=True, help='Folio host')
    p.add_argument('-l', '--list', action='store', type=int,
                   nargs='+', help='list of bib ids')
    p.add_argument('-f', '--file', action='store',
                   help='Name of file of bib ids')
    p.add_argument('-v', '--validate', action='store_true',
                   help='Validate rows')
    p.add_argument('-u', '--update_uuids', action='store_true',
                   help='Update uuids')
    p.add_argument('-p', '--public', action='store_true',
                   help='Create inventory for public')
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
        self.folio_host = self.args.folio_host
        self.validate = self.args.validate
        self.update_uuids = self.args.update_uuids
        self.public = self.args.public
        self.mdb = None
        self.pdb = None
        self.col = dict()
        self.map = dict()
        self.vern = None
        self.jo = dict()
        self.bib_ids = []
        self.batch_size = 1000
        self.cur_batch = []
        self.b = None
        self.bd = None
        self.bl = None
        self.bct1 = 0
        self.bct2 = 0
        self.h = None
        self.hd = None
        self.hl = None
        self.hct1 = 0
        self.hct2 = 0
        self.i = None
        self.id = None
        self.il = None
        self.ict1 = 0
        self.ict2 = 0
        self.m = None
        self.md = None
        self.ml = None
        self.mct1 = 0
        self.mct2 = 0
        self.tmp_path = Path('/tmp')
        self.cur_path = Path.cwd()
        self.tag_map_filename = './tag_map.txt'
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
        self.cur_time = pendulum.now('utc')
        self.cur_user = 'arnt'
        self.cur_user_id = None
        self.save_ref_tables = True
        self.ref = dict()
        self.ref_dict = dict()
        self.schema = dict()
        self.fetch_latest_json_schemas = False
        self.validate_instance = None
        self.validate_holdings = None
        self.validate_item = None
        self.create_uuid_table = False
        self.uuid = dict()
        self.tables = []
        self.indexes = []
        self.index_dict = None
        self.index_tables = ['instance', 'holdings_record', 'item']
        self.int_pat = re.compile('\d+')
        self.pat = re.compile("<record>.*</record>", re.DOTALL)
        self.vern_pat = re.compile('^\d\d\d-\d\d')
        self.contrib_pat = re.compile('"')
        self.digits = list(string.digits)
        self.alpha_digits = list('%s%s' % (self.digits,string.ascii_lowercase))
        self.bib_jo = []
        self.holdings_jo = []
        

def main():
    ts = time()
    m = main_class()
    initialize_logging(m)
    open_reject_file(m)
    m.mdb = mdb_class(m)
    m.pdb = pdb_class(m)
    map_uuids(m)
    fetch_index_defs(m)
    initialize_pre_load(m)
    m.log.info('Time started: %s' % ctime())
    try:
        truncate_rows(m)
        load_reference_data(m)
        fetch_bib_ids(m)
        fetch_col_names(m)
        fetch_tag_maps(m)
        # fetch_json_schemas(m)
        load_json_schemas(m)
        load_recs(m)
    except:
        m.log.error("Fatal error", exc_info=m.stack_trace)
    finally:
        initialize_post_load(m)
        m.mdb.cur.close()
        m.mdb.con.close()
        m.pdb.cur.close()
        m.pdb.con.close()
        close_reject_file(m)
        te = time()
        log_perf_metrics(m)
        m.log.info('Time ended: %s' % ctime())
        m.log.info('Time taken: %2.3f secs' % (te-ts))
        return m


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
            m.log.debug(f"{f.__name__} took: {round(te-ts,2)} secs")
        else:
            result = f(*args, **kw)
        return result
    return wrap


def log_perf_metrics(m):
    hdr = ['                 Function name', '  Total time', '  Average Time', '  Count']
    a,b,c,d = [str(len(s)) for s in hdr]
    format_str = '%'+a+'s%'+b+'.3f%'+c+'.3f%'+d+'d'
    num_lst = [[k,v[0],v[1],v[2]] for k,v in list(m.perf.items())]
    str_lst = [''.join(hdr)] + [format_str % tuple(l) for l in num_lst]
    m.log.info("Performance times by function:")
    for s in str_lst: m.log.info(s)
    hdr = ['                   Record Type', '   Processed', '      Rejected']
    a,b,c = [str(len(s)) for s in hdr]
    format_str = '%'+a+'s%'+b+'d%'+c+'d'
    num_lst = [['Bibs', m.bct1, m.bct1 - m.bct2],
               ['Holdings', m.hct1, m.hct1 - m.hct2],
               ['Items', m.ict1, m.ict1 - m.ict2]]
    str_lst = [''.join(hdr)] + [format_str % tuple(l) for l in num_lst]
    m.log.info("Number of records processed:")
    for s in str_lst: m.log.info(s)
    m.log.info("See loader.log and reject.log for details")


##### bibs


@timing
def fetch_bib_ids(m):
    pat = re.compile('\d+')
    if m.args.list:
        m.bib_ids = create_batches(m, m.args.list)
    elif m.args.file:
        with open(m.args.file, 'r') as f:
            l = f.readlines()
            l = [int(i.strip()) for i in l if re.match(pat, i.strip())]
            m.bib_ids = create_batches(m, l)
    m.log.debug("fetched bib_ids")


@timing
def load_recs(m):
    btime = time()
    for batch in m.bib_ids:
        m.cur_batch = batch
        fetch_bib_rows(m)
        map_bib_rows(m)
        validate_ins_rows(m)
        format_ins_rows(m)
        save_ins_rows(m)
        m.log.info("bibs processed: %d" % m.bct1)

        # map_marc_rows(m)
        # validate_marc_rows(m)
        # format_marc_rows(m)
        # save_marc_rows(m)
        # m.log.info("bibs processed: %d" % m.bct1)
        
        fetch_holdings_rows(m)
        fetch_holdings_note_rows(m)
        fetch_holdings_uri_rows(m)
        fetch_holdings_statistical_code_rows(m)
        fetch_extent_of_ownership_rows(m)
        map_holdings_rows(m)
        validate_holdings_rows(m)
        format_holdings_rows(m)
        save_holdings_rows(m)
        m.log.debug("holdings processed: %d" % m.hct1)

        fetch_item_rows(m)
        fetch_item_former_ids(m)
        fetch_item_note_rows(m)
        fetch_item_statistical_code_rows(m)
        fetch_item_order_item_rows(m)
        map_item_rows(m)
        validate_item_rows(m)
        format_item_rows(m)
        save_item_rows(m)
        m.log.debug("items processed: %d" % m.ict1)
    m.log.info("completed loading recs")
    

def new_row(row):
    return dict([['include', True],
                 ['error', None],
                 ['o', dict([['r', row],
                             ['d', None]])],
                 ['f', dict([['r', None],
                             ['d', None]])],
                 ['j', dict([['r', None],
                             ['d', None]])],
                 ['r', dict([['r', None],
                             ['d', None]])]])


@timing
def fetch_bib_rows(m):
    m.bl = []
    if m.cur_batch:
        cols = m.col['ole_ds_bib_t'] + ['uuid']
        s = ','.join(['%s'] * len(m.cur_batch))
        sql = "select b.*, u.uuid from ole_ds_bib_t b " \
              "join cat.uuid u on b.bib_id = u.id " \
              "where u.table_name = 'ole_ds_bib_t' " \
              "and b.bib_id in (%s) " \
              "and b.status != '%s' " % (s, 'Do not migrate')
        m.mdb.cur.execute(sql, m.cur_batch)
        m.bl = [new_row(dict([[c,v] for c,v in zip(cols,r)])) \
                for r in m.mdb.cur.fetchall()]
        m.bd = dict([[b['o']['r']['bib_id'], b] for b in m.bl])
        m.bct1 = m.bct1 + len(m.bl)
        m.log.debug("fetched %d bib rows" % len(m.bl))


@timing
def map_bib_rows(m):
    for b in m.bl:
        m.b = b
        r = b['o']['r']
        try:
            map_bib_doc(m)
            create_ins_doc(m)
            create_ins_row(m)
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
            if b['include']:
                e = 'unable to map bib row: %d' % r['bib_id']
                m.log.error(e, exc_info=m.stack_trace)
                m.log.info(pformat(r))
                b['include'] = False

        
# @timing
def map_bib_doc(m):
    btime = time()
    for jo in m.jo.keys(): m.jo[jo] = []
    parse_marcxml(m)
    for a in m.b['o']['d']:
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
    m.b['o']['r']['jo'] = dict(list(m.jo.items()))
    

#@timing
def parse_marcxml(m):
    m.b['o']['d'] = etree.fromstring(re.search(m.pat, m.b['o']['r']['content']).group())


#@timing
def create_ins_doc(m):
    m.b['f']['d'] = dict()
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
    # Good
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
    # Good
    # use bib_id in new model?
    m.b['f']['d']['id'] = m.b['o']['r']['uuid']

    
def map_hrid(m):
    # Good
    # from marc 001
    m.b['f']['d']['hrid'] = 'in' + str(m.b['o']['r']['bib_id'])

    
def map_source(m):
    # Done
    m.b['f']['d']['source'] = 'MARC'

    
def map_title(m):
    # Good
    # There is a len limit on title of 2650 until folio index is fixed.
    # Has workaround for missing title
    # Check to see how to handle parens in the case where there is no title
    # but there is a vernacular
    s = None
    l = (m.jo['title'] + [[],[]])[:2]
    if l[0]: s = ''.join([d['content'] for d in l[0]])
    if l[1]: s = '%s (%s)' % (s, ''.join([d['content'] for d in l[1]]))
    m.b['f']['d']['title'] = s[:2650] if s else ''
        
    
def map_index_title(m):
    # Good
    # There is a len limit on index_title of 2650 until folio index is fixed.
    # Use the ind2 value to offset the start of the 245
    s = None
    l = (m.jo['title'] + [[],[]])[:2]
    if l[0]: 
        ind = int(l[0][0]['ind2']) if l[0][0]['ind2'] in m.digits else 0
        s = ''.join([d['content'] for d in l[0]])[ind:]
    if l[1]:
        s = '%s (%s)' % (s, ''.join([d['content'] for d in l[1]]))
    if s: m.b['f']['d']['indexTitle'] = s[:2650]


def map_alternative_titles(m):
    # Good
    # Use UC mapping for this category
    # I had to dedup these to pass validation
    a = dict([['130', 'Uniform Title'],
              ['240', 'Uniform Title'],
              ['246', 'Variant Title'],
              ['247', 'Former Title']]) 
    acc = []
    for l in [l for l in m.jo['alternativeTitles'] if l]:
        if l[0]['tag'] in a:
            uuid = m.ref['alternative_title_type'][a[l[0]['tag']]]
            alt_title = ''.join([d['content'] for d in l])
            j = dict([['alternativeTitleTypeId', uuid],
                      ['alternativeTitle', alt_title]])
            if j not in acc: acc.append(j)
    if acc: m.b['f']['d']['alternativeTitles'] = acc


def map_editions(m):
    # Good
    l = [''.join([d['content'] for d in l]) for l in m.jo['editions']]
    l = reduce(lambda x,y: x+[y] if not y in x else x, l, [])
    if l: m.b['f']['d']['editions'] = l
    
    
def map_series(m):
    # Good
    # List of 830, 490 and 440s
    l = [''.join([d['content'] for d in l]) for l in m.jo['series']]
    l = reduce(lambda x,y: x+[y] if not y in x else x, l, [])
    if l: m.b['f']['d']['series'] = l

    
def map_identifiers(m):
    # Done
    # Fixed reference table
    # Add 035 that has prefix '(OCoLC)' in 'a' or 'z'
    # If 'a', use 'OCLC' identifier. If 'z', use 'Cancelled OCLC' identifier
    i = {'010':{'a':'LCCN', 'z':'Invalid LCCN'},
         '020':{'a':'ISBN', 'z':'Invalid ISBN'},
         '022':{'a':'ISSN', 'z':'Invalid ISSN', 'l':'Linking ISSN'},
         '024':{'a':'Other Standard Identifier', 'z':'Cancelled Other Standard Identifier'},
         '028':{'a':'Publisher or Distributor Number', 'z':'Cancelled Publisher or Distributor Number'},
         '035':{'a':'System Control Number', 'z':'Cancelled System Control Number'},
         '035a':{'a':'OCLC', 'z':'Cancelled OCLC'},
         '074':{'a':'GPO Item Number', 'z':'Cancelled GPO Item Number'}}
    acc = []
    for d in [d for l in m.jo['identifiers'] for d in l]:
        tag, sf, v = (d['tag'], d['sf'], d['content'])
        if tag in ['035'] and sf in ['a','z'] and v[:7] == '(OCoLC)': tag = '035a'
        if tag in i and sf in ('a','z','l'):
            uuid = m.ref['identifier_type'][i[tag][sf]]
            j = {'identifierTypeId': uuid, 'value': v}
            acc.append(j)
    if acc: m.b['f']['d']['identifiers'] = acc
            

def map_contributors(m):
    # Done
    # Need to work with Christie on conditional logic and cleanup issues
    # Only sfs e,j,4 are contributor types. If absent concat content fields
    n = {'100': {'primary': True, 'type': 'Personal name'},
         '110': {'primary': True, 'type': 'Corporate name'},
         '111': {'primary': True, 'type': 'Meeting name'},
         '700': {'primary': False, 'type': 'Personal name'},
         '710': {'primary': False, 'type': 'Corporate name'},
         '711': {'primary': False, 'type': 'Meeting name'}}
    acc = []
    for l in [l for l in m.jo['contributors'] if l]:
        c = {}
        acc2 = []
        for d in l:
            c['primary'] = n[d['tag']]['primary']
            c['contributorNameTypeId'] = m.ref['contributor_name_type'][n[d['tag']]['type']]
            if d['sf'] in ['e']:
                name = d['content'].capitalize().strip('., ')
                if name in m.ref_dict['contributor_type']:
                    c['contributorTypeId'] = m.ref_dict['contributor_type'][name]['id']
            elif d['sf'] in ['j','4']:
                code = d['content']
                if code in m.ref['contributor_type']:
                    c['contributorTypeId'] = m.ref['contributor_type'][code]
            else: acc2.append(''.join(re.split(m.contrib_pat, d['content'])))
        c['name'] = ' '.join (acc2)
        acc.append(c)
    if acc: m.b['f']['d']['contributors'] = acc


def map_subjects(m):
    # Todo
    # Subjects are screwed up. Talk to Christie
    # Todo: add check for prefix of lc on sf 2
    # how to I handle 880s here?
    # 600 - 655 only include if ind2 is 0,2,7, the latter only if sf 2 == 'fast'
    # or if sf 2 begins with 'lc'
    # Only concatenate alphbetic subfields, ie, exclude '0' and '7'.
    acc = []
    for l in m.jo['subjects']:
        tagp = [d for d in l if d['tag'] >= '600' and d['tag'] <='655']
        indp = [d for d in l if tagp and d['ind2'] in ('0','2')]
        fastp = [d for d in l if tagp and d['ind2'] == '7' and d['sf'] == '2' and d['content'] == 'fast']
        lcp = [d for d in l if tagp and d['ind2'] == '7' and d['sf'] == '2' \
                 and len(d['content']) > 1 and d['content'][:2] == 'lc']
        if tagp and indp: acc.append('--'.join([d['content'] for d in l if d['sf'] not in m.digits]))
        elif tagp and fastp: acc.append('--'.join([d['content'] for d in l if d['sf'] not in m.digits]))
        elif tagp and lcp: acc.append('--'.join([d['content'] for d in l if d['sf'] not in m.digits]))
        elif not tagp: acc.append('--'.join([d['content'] for d in l if d['sf'] not in m.digits]))
        acc = reduce(lambda x, y: x + [y] if not y in x else x, acc, [])
        if acc: m.b['f']['d']['subjects'] = acc


def map_classifications(m):
    # Good
    a = {'050': 'Library of Congress Classification',
         '082': 'Dewey Decimal Classification',
         '086': 'Government Document Classification',  
         '090': 'Library of Congress Classification (Local)'}
    acc = []
    for l in [l for l in m.jo['classification'] if l]:
        b = l[0]
        if b['tag'] in a:
            c = dict([['classificationNumber', ' '.join([s for s in [d['content'] for d in l]])],
                      ['classificationTypeId',  m.ref['classification_type'][a[b['tag']]]]])
            acc.append(c)
    if acc: m.b['f']['d']['classifications'] = acc


def map_publications(m):
    # Good
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
    if acc: m.b['f']['d']['publication'] = acc


def map_publication_frequency(m):
    # Good
    l = [''.join([d['content'] for d in l]) for l in m.jo['publicationFrequency']]
    l = reduce(lambda x,y: x+[y] if not y in x else x, l, [])
    if l: m.b['f']['d']['publicationFrequency'] = l


def map_publication_range(m):
    # Good
    l = [''.join([d['content'] for d in l]) for l in m.jo['publicationRange']]
    l = reduce(lambda x,y: x+[y] if not y in x else x, l, [])
    if l: m.b['f']['d']['publicationRange'] = l

    
def map_electronic_access(m):
    # Good
    # Includes work around for required 'uri'
    ind = dict([[' ','No information provided'],
                ['0','Resource'],
                ['1','Version of resource'],
                ['2','Related resource'],
                ['8','No display constant generated']])
    acc = []
    m.bib_jo.append(m.jo['electronicAccess'])
    for l in m.jo['electronicAccess']:
        e = dict()
        for d in l:
            if d['sf'] == 'u': e['uri'] = d['content']
            if d['sf'] == 'y': e['linkText'] = d['content']
            if d['sf'] == '3': e['materialsSpecification'] = d['content']
            if d['sf'] == 'z': e['publicNote'] = d['content']
            if d['ind2'] in (' ','0','1','2','8'):
                e['relationshipId'] = m.ref['electronic_access_relationship'][ind[d['ind2']]]
        if e and 'uri' not in e: e['uri'] = ''
        if e: acc.append(e)
    if acc: m.b['f']['d']['electronicAccess'] = acc

    
def map_instance_type(m):
    # Done
    # Pull from 336. Name will be in sf a which is a name.
    # If not sf a, look sf b which is a code
    # for code and pull id from reference table from code.
    # If no 336, use name of 'text'
    # This is a required field. There is temporary placeholder for nulls.
    for l in m.jo['instance_type']:
        for d in l:
            if d['sf'] == 'a' and d['content'] in m.ref['instance_type']:
                m.b['f']['d']['instanceTypeId'] = m.ref['instance_type'][d['content']]
            elif d['sf'] == 'b' and d['content'] in m.ref_dict['instance_type']:
                m.b['f']['d']['instanceTypeId'] = m.ref_dict['instance_type'][d['content']]['id']
            else: m.b['f']['d']['instanceTypeId'] = m.ref['instance_type']['text']

                
def map_instance_format(m):
    # Done
    # Fix up loops
    # reference data is identical
    acc = []
    for d in [d for l in m.jo['instance_format'] for d in l]:
        if d['sf']=='b' and d['content'] in m.ref['instance_format']:
            acc.append(m.ref['instance_format'][d['content']])
    if acc: m.b['f']['d']['instanceFormatIds'] = acc


def map_physical_descriptions(m):
    # good
    l = [' '.join(l) for l in [[d['content'] for d in l] for l in m.jo['physicalDescriptions']]]
    if l: m.b['f']['d']['physicalDescriptions'] = l


def map_languages(m):
    # good
    l = [d['content'] for l in m.jo['languages'] for d in l if d['tag'] in ['041']] or \
        [d['content'][35:38] for l in m.jo['languages'] for d in l if d['tag'] in ['008']]
    if l: m.b['f']['d']['languages'] = l


def map_notes(m):
    # Good
    d = dict([['500', 'General note'],
              ['501', 'With note'],
              ['502', 'Dissertation note'],
              ['504', 'Bibliography note'],
              ['505', 'Formatted Contents Note'],
              ['506', 'Restrictions on Access note'],
              ['507', 'Scale note for graphic material'],
              ['508', 'Creation / Production Credits note'],
              ['510', 'Citation / References note'],
              ['511', 'Participant or Performer note'],
              ['513', 'Type of report and period covered note'],
              ['514', 'Data quality note'],
              ['515', 'Numbering peculiarities note'],
              ['516', 'Type of computer file or data note'],
              ['518', 'Date / time and place of an event note'],
              ['520', 'Summary'],
              ['522', 'Geographic Coverage note'],
              ['524', 'Preferred Citation of Described Materials note'],
              ['525', 'Supplement note'],
              ['530', 'Additional Physical Form Available note'],
              ['532', 'Accessibility note'],
              ['533', 'Reproduction note'],
              ['534', 'Original Version note'],
              ['540', 'Terms Governing Use and Reproduction note'],
              ['541', 'Immediate Source of Acquisition note'],
              ['542', 'Information related to Copyright Status'],
              ['544', 'Location of Other Archival Materials note'],
              ['545', 'Biographical or Historical Data'],
              ['546', 'Language note'],
              ['547', 'Former Title Complexity note'],
              ['550', 'Issuing Body note'],
              ['552', 'Entity and Attribute Information note'],
              ['555', 'Cumulative Index / Finding Aides notes'],
              ['556', 'Information About Documentation note'],
              ['561', 'Ownership and Custodial History note'],
              ['562', 'Copy and Version Identification note'],
              ['563', 'Binding Information note'],
              ['565', 'Case File Characteristics note'],
              ['567', 'Methodology note'],
              ['580', 'Linking Entry Complexity note'],
              ['583', 'Action note'],
              ['586', 'Awards note'],
              ['590', 'Local notes'],
              ['591', 'Local notes'],
              ['592', 'Local notes'],
              ['593', 'Local notes'],
              ['594', 'Local notes'],
              ['595', 'Local notes'],
              ['596', 'Local notes'],
              ['597', 'Local notes'],
              ['598', 'Local notes'],
              ['599', 'Local notes']])
    acc = []
    for l in m.jo['notes']:
        if l and l[0]:
            tag = l[0]['tag']
            j = {'instanceNoteTypeId': m.ref['instance_note_type'][d[tag]],
                 'note': ' '.join([d['content'] for d in l]),
                 'staffOnly': True if m.b['o']['r']['staff_only'] == 'T' else False}
            acc.append(j)
    if acc: m.b['f']['d']['notes'] = acc
    # l = [' '.join(l) for l in [[d['content'] for d in l] for l in m.jo['notes']]]


def map_mode_of_issuance(m):
    # Done
    # Code change done, but needs testing
    # from marc LDR/07
    # see https://www.oclc.org/bibformats/en/fixedfield/blvl.html
    d = dict([['a','Monographic component part'],
              ['b','Serial component part'],
              ['c','Collection'],
              ['d','Subunit'],
              ['i','Integrating resource'],
              ['m','Monograph'],
              ['s','Serial']])
    code = m.jo['mode_of_issuance'][0][0]['content'][7]
    if code in d:
        m.b['f']['d']['modeOfIssuanceId'] = m.ref['mode_of_issuance'][d[code]]
    else: m.b['f']['d']['modeOfIssuanceId'] = m.ref['mode_of_issuance'][d['m']]

    
def map_catalog_date(m):
    # This should remain null
    # m.b['f']['d']['catalogedDate'] = None
    pass

    
def map_previously_held(m):
    # This should be False
    # m.b['f']['d']['previouslyHeld'] = False
    pass

    
def map_staff_suppress(m):
    # This should be False
    # m.b['f']['d']['staffSuppress'] = False
    pass

    
def map_discovery_suppress(m):
    # Good
    # map from staff_only in bib rec; convert to True/False
    flag = {'Y':True, 'N':False, None: False}[m.b['o']['r']['staff_only']]
    if m.public and flag == True:
        m.b['include'] = False
        raise Exception ("%s is not for public use" % m.b['o']['r']['bib_id']) 
    if flag: m.b['f']['d']['discoverySuppress'] = flag

    
def map_statistical_codes(m):
    # Done
    # map from 901 with values of ASER, ISER, ESER, and
    #type values of 'Serial status'
    acc = []
    for d in [d for l in m.jo['statistical_code'] for d in l if d]:
        if d['content'] in m.ref['statistical_code']:
            acc.append(m.ref['statistical_code'][d['content']])
    acc = reduce(lambda x,y: x+[y] if not y in x else x, acc, [])
    if acc: m.b['f']['d']['statisticalCodeIds'] = acc

    
def map_source_record_format(m):
    # Good
    # can't find this in the record output; why not?
    m.b['f']['d']['sourceRecordFormat'] = "MARC-JSON"

    
def map_status(m):
    # Fix typo in status in both ole and folio: 'Batch record ...'
    # use instance_status and map from m.b['o']['r']['status']
    exclude = ['Batch record load no export permited',
               'Electronic resource temporary',
               'Table of Contents from BNA']
    if m.b['o']['r']['status'] in m.ref['instance_status']:
        m.b['f']['d']['statusId'] = m.ref['instance_status'][m.b['o']['r']['status']]
        if m.public and m.b['o']['r']['status'] in exclude:
            m.b['include'] = False
            raise Exception ("%s is not for public use" % m.b['o']['r']['bib_id']) 

    
def map_status_update_date(m):
    # Good
    date = date_to_str(m.b['o']['r']['status_updated_date'])
    if date: m.b['f']['d']['statusUpdatedDate'] = date

    
def map_metadata(m):
    meta = add_metadata(m, created_by = m.b['o']['r']['created_by'],
                        date_created = m.b['o']['r']['date_created'],
                        updated_by = m.b['o']['r']['updated_by'],
                        date_updated = m.b['o']['r']['date_updated'])
    m.b['f']['d']['metadata'] = meta

    
#@timing
def create_ins_row(m):
    if 'createdByUserId' in m.b['f']['d']['metadata']:
        created_by = m.b['f']['d']['metadata']['createdByUserId']
    else: created_by = None
    d = dict([['_id', m.b['o']['r']['uuid']],
              ['jsonb', m.b['f']['d']],
              ['creation_date', loc_to_utc(m.b['o']['r']['date_created'])],
              ['created_by', created_by],
              ['instancestatusid', m.b['f']['d']['statusId'] \
               if 'statusId' in m.b['f']['d'] else None],
              ['modeofissuanceid', m.b['f']['d']['modeOfIssuanceId'] \
               if 'modeOfIssuanceId' in m.b['f']['d'] else None],
              ['instancetypeid', m.b['f']['d']['instanceTypeId'] \
               if 'instanceTypeId' in m.b['f']['d'] else None],
              ['bib_id', m.b['o']['r']['bib_id']]])
    m.b['f']['r'] = d


@timing
def validate_ins_rows(m):
    if m.validate:
        for b in [b for b in m.bl if b['include']]:
            r = b['f']['r']
            try:
                # jsonschema.validate(instance=r['jsonb'], schema=m.schema['instance'])
                m.validate_instance(r['jsonb'])
            except:
                m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
                m.log.error("error in instance row %s" % r['bib_id'], exc_info=m.stack_trace)
                b['include'] = False

            
@timing
def format_ins_rows(m):
    for b in [b for b in m.bl if b['include']]:
        r = b['f']['r']
        try:
            r['jsonb'] = json.dumps(r['jsonb'], default=serialize_datatype)
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
            m.log.error("error in instance row %s" % r['bib_id'], exc_info=m.stack_trace)
            b['include'] = False

            
@timing
def save_ins_rows(m):
    l = [b for b in m.bl if b['include']]
    if l:
        count = len(l)
        cols = m.col['instance']
        sql = "insert into instance values %s"
        rows = [list(b['f']['r'].values())[:-1] for b in l]
        template = '(%s)' % ', '.join(['%s'] * len(cols))
        try:
            execute_values(m.pdb.cur, sql, rows, template=template, page_size=count)
            m.pdb.con.commit()
            m.log.debug("saved instance rows")
        except:
            m.pdb.con.rollback()
            sql = "insert into instance values (%s)" % ','.join(['%s'] * len(cols))
            for b in l:
                r = b['f']['r']
                try:
                    m.pdb.cur.execute(sql, list(r.values())[:-1])
                    m.pdb.con.commit()
                except:
                    m.pdb.con.rollback()
                    r['jsonb'] = json.loads(r['jsonb'])
                    m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
                    e = "error in instance row %s" % r['bib_id']
                    m.log.error(e, exc_info=m.stack_trace)
                    m.log.debug(pformat(r))
                    b['include'] = False
        finally:
            m.bct2 = m.bct2 + len([b for b in m.bl if b['include']])
        

##### holdings


@timing
def fetch_holdings_rows(m):
    m.hl = []
    bib_ids = [b['o']['r']['bib_id'] for b in m.bl if b['include']]
    if bib_ids:
        s = ','.join(['%s'] * len(bib_ids))
        # l = "','".join(exclude_locations(m))
        cols = m.col['ole_ds_holdings_t'] + ['uuid', 'bib_uuid']
        sql = "select h.*, u.uuid, v.uuid from ole_ds_holdings_t h " \
              "join cat.uuid u on h.holdings_id = u.id " \
              "join cat.uuid v on h.bib_id = v.id " \
              "where u.table_name = 'ole_ds_holdings_t' " \
              "and v.table_name = 'ole_ds_bib_t' " \
              "and h.bib_id in (%s) " % s
        m.mdb.cur.execute(sql, bib_ids)
        m.hl = [new_row(dict([[c,v] for c,v in zip(cols,r)])) \
               for r in m.mdb.cur.fetchall()]
        m.hd = dict([[h['o']['r']['holdings_id'], h] for h in m.hl])
        m.hct1 = m.hct1 + len(m.hl)
        m.log.debug("fetched %d holdings rows" % len(m.hl))


@timing
def fetch_holdings_note_rows(m):
    if m.hl:
        holdings_ids = [h['o']['r']['holdings_id'] for h in m.hl]
        cols = m.col['ole_ds_holdings_note_t']
        s = ','.join(['%s'] * len(holdings_ids))
        sql = "select * from ole_ds_holdings_note_t where holdings_id in (%s)" % s
        m.mdb.cur.execute(sql, holdings_ids)
        note_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for h in m.hl: h['o']['r']['notes'] = []
        for d in note_rows:
            if d['note']: m.hd[d['holdings_id']]['o']['r']['notes'].append(d)
        m.log.debug("fetched %d holdings note rows" % len(note_rows))


@timing
def fetch_holdings_uri_rows(m):
    if m.hl:
        holdings_ids = [h['o']['r']['holdings_id'] for h in m.hl]
        cols = m.col['ole_ds_holdings_uri_t']
        s = ','.join(['%s'] * len(holdings_ids))
        sql = "select * from ole_ds_holdings_uri_t where holdings_id in (%s)" % s
        m.mdb.cur.execute(sql, holdings_ids)
        uri_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for h in m.hl: h['o']['r']['uris'] = []
        for d in uri_rows:
            if d['uri']: m.hd[d['holdings_id']]['o']['r']['uris'].append(d)
        m.log.debug("fetched %d holdings uri rows" % len(uri_rows))


@timing
def fetch_holdings_statistical_code_rows(m):
    if m.hl:
        holdings_ids = [h['o']['r']['holdings_id'] for h in m.hl]
        cols = m.col['ole_ds_holdings_stat_search_t']
        s = ','.join(['%s'] * len(holdings_ids))
        sql = "select * from ole_ds_holdings_stat_search_t where holdings_id in (%s)" % s
        m.mdb.cur.execute(sql, holdings_ids)
        stat_cd_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for h in m.hl: h['o']['r']['statistical_codes'] = []
        for d in stat_cd_rows:
            if d['stat_search_code_id']: m.hd[d['holdings_id']]['o']['r']['statistical_codes'].append(d)
        m.log.debug("fetched %d statistical_codes rows" % len(stat_cd_rows))
                                                                

@timing
def fetch_extent_of_ownership_rows(m):
    if m.hl:
        holdings_ids = [h['o']['r']['holdings_id'] for h in m.hl]
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
        
        for h in m.hl: h['o']['r']['ext_ownership'] = []
        for d in ext_rows:
            if d['text']: m.hd[d['holdings_id']]['o']['r']['ext_ownership'].append(d)
        m.log.debug("fetched %d extent of ownership rows" % len(ext_rows))

                        
def add_metadata(m, created_by=None, date_created=None, updated_by=None, date_updated=None):
    d = dict()
    if created_by and not m.public:
        d['createdByUsername'] = created_by
        if created_by in m.ref['user']:
            d['createdByUserId'] = m.ref['user'][created_by]
    if date_created:
        d['createdDate'] = date_to_str(date_created)
    else: d['createdDate'] = date_to_str(m.cur_time)
    if updated_by and not m.public:
        d['updatedByUsername'] = updated_by
        if updated_by in m.ref['user']:
            d['updatedByUserId'] = m.ref['user'][updated_by]
    if date_updated:
        d['updatedDate'] = date_to_str(date_updated)
    else: d['updatedDate'] = date_to_str(m.cur_time)
    return d
                        

@timing
def map_holdings_rows(m):
    for h in m.hl:
        m.h = h
        r = h['o']['r']
        try:
            create_holdings_doc(m)
            create_holdings_row(m)
        except:
            ln = '%s\t%s\t%s\n' % (r['bib_id'], r['holdings_id'], None)
            m.reject_file.write(ln)
            if h['include']:
                e = 'unable to map holdings: %d' % r['holdings_id']
                m.log.error(e, exc_info=m.stack_trace)
                m.log.info(pformat(r))
                h['include'] = False


#@timing
def create_holdings_doc(m):
    m.h['f']['d'] = dict()
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
    map_holdings_discovery_suppress(m)
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
    m.h['f']['d']['id'] = m.h['o']['r']['uuid']


def map_holdings_instance_id(m):
    """
    :desc: map previous holdings ids to Folio formerIds
    :source: (str) ole_ds_holdings_t.former_holdings_id
    :target: (list) holdingsrecord.formerIds
    :note: With OLE, there is only a single value to migrate, unless we include the OLE holdings_id
    """
    m.h['f']['d']['instanceId'] = m.h['o']['r']['bib_uuid']


def map_holdings_hrid(m):
    """
    :desc: map OLE holdings id to Folio hrid
    :source: ole_ds_holdings_t.holdings_id
    :target: holdingsrecord.hrid
    :note: Find and set the starting number as max(hrid)+1 for the system-assigned hrid.
    """
    if m.h['o']['r']['holdings_id']:
        m.h['f']['d']['hrid'] = 'ho' + str(m.h['o']['r']['holdings_id'])


def map_holdings_type_id(m):
    """
    :desc: map OLE holdings type to Folio holdings type
    :source: ole_ds_holdings_t.holdings_type
    :target: holdindgsrecord.holdingsTypeId
    :trans: 'electronic' to 'electronic' and 'print' to 'physical'
    """
    d = {'print':'physical','electronic':'electronic'}
    if m.h['o']['r']['holdings_type'] in d:
        id = m.ref['holdings_type'][d[m.h['o']['r']['holdings_type']]]
        m.h['f']['d']['holdingsTypeId'] = id


def map_holdings_former_ids(m):
    """
    :desc: map previous holdings ids to Folio formerIds
    :source: (str) ole_ds_holdings_t.former_holdings_id
    :target: (list) holdingsrecord.formerIds
    :note: With OLE, there is only a single value to migrate, 
    :unless we include the OLE holdings_id
    """
    v = m.h['o']['r']['former_holdings_id']
    if v: m.h['f']['d']['formerIds'] = [v]

    
def map_holdings_permanent_location(m):
    # Need a permanent location value for the db column
    d = {'ASR': 1, 'JRL': 2, 'JCL': 3, 'SPCL': 4, 'ITS': 5,
         'Online': 6, 'UCX': 7, 'AANet': 8, 'Eck': 9, 'SSAd': 10,
         'DLL': 11, 'MCS': 12, 'POLSKY': 13, 'LMC': 14, 'unk': 15}
    s = m.h['o']['r']['location']
    perm_id = 'permanentLocationId'
    if s and len(s.split('/')) == 3:
        ins, lib, shv = s.split('/')
        loc = 'UC/HP/%s/%s' % (lib, shv)
        if loc in m.ref['location']:
            m.h['f']['d'][perm_id] = m.ref['location'][loc]
        else: m.h['f']['d'][perm_id] = m.ref['location']['UC/HP/JRL/Gen']
    else: m.h['f']['d'][perm_id] = m.ref['location']['UC/HP/JRL/Gen']

    
def map_holdings_temporary_location(m):
    # Need a temporary location value for the db column
    # m.fh_doc['temporaryLocationId'] = None
    pass


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
    if m.h['o']['r']['call_number_type_id'] in d:
        name = d[m.h['o']['r']['call_number_type_id']]
        m.h['f']['d']['callNumberTypeId'] = m.ref['call_number_type'][name]


def map_holdings_call_number_prefix(m):
    v = m.h['o']['r']['call_number_prefix']
    if v: m.h['f']['d']['callNumberPrefix'] = v


def map_holdings_call_number(m):
    v =  m.h['o']['r']['call_number']
    if v: m.h['f']['d']['callNumber'] = v


def map_holdings_call_number_suffix(m):
    # m.h['f']['d']['callNumberSuffix'] = None
    pass


def map_holdings_shelving_title(m):
    # m.h['f']['d']['shelvingTitle'] = None
    pass


def map_holdings_acquisition_format(m):
    # m.h['f']['d']['acquisitionFormat'] = None
    pass


def map_holdings_acquisition_method(m):
    # m.h['f']['d']['acquisitionMethod'] = None
    pass


def map_holdings_notes(m):
    # staff_only defaults to False
    acc = []
    r = m.h['o']['r']
    for d in r['notes']:
        if not (m.public and d['type'] == 'nonPublic'):
            if r['holdings_type'] == 'print':
                j = dict([['holdingsNoteTypeId', m.ref['holdings_note_type']['Note']],
                          ['note', d['note']],
                          ['staffOnly', True if d['type'] == 'nonPublic' else False]])
                acc.append(j)
            elif r['holdings_type'] == 'electronic' and d['type'] == 'nonPublic':
                j = dict([['holdingsNoteTypeId', m.ref['holdings_note_type']['Note']],
                          ['note', d['note']],
                          ['staffOnly', True]])
                acc.append(j)
    if acc: m.h['f']['d']['notes'] = acc
    
    
def map_holdings_ill_policy_id(m):
    # a value is required for the holdings_record row; use placeholder for now
    # Christie will talk to David L. and Clara F.
    m.h['f']['d']['illPolicyId'] = m.ref['ill_policy']['undefined']


def map_holdings_retention_policy(m):
    # m.h['f']['d']['retentionPolicy'] = None
    pass


def map_holdings_digitization_policy(m):
    # m.h['f']['d']['digitizationPolicy'] = None
    pass


def map_holdings_electronic_access(m):
    # Turned loading back on
    # Go over this with Christie
    ind = dict([[' ','No information provided'],
                ['0','Resource'],
                ['1','Version of resource'],
                ['2','Related resource'],
                ['8','No display constant generated']])
    acc = []
    jo = m.bd[m.h['o']['r']['bib_id']]['o']['r']['jo']
    for l in jo['electronicAccess']:
        uri_from_holdings = [u['uri'] for u in m.h['o']['r']['uris']]
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
    if acc: m.h['f']['d']['electronicAccess'] = acc

    
def map_holdings_discovery_suppress(m):
    # map from staff_only in holdings rec; convert to True/False
    staff_only = {'Y':True, 'N':False}[m.h['o']['r']['staff_only']]
    if staff_only: m.h['f']['d']['discoverySuppress'] = staff_only
    if m.public and staff_only == True:
        m.h['include'] = False
        e = "Holdings id %s is not for public use" % m.h['o']['r']['holdings_id']
        raise Exception (e)

    
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
              [56, 'polsky'],
              [57, 'eintegrating']])
    l = [d[a['stat_search_code_id']] for a in m.h['o']['r']['statistical_codes']]
    l = [m.ref['statistical_code'][a] for a in l]
    l = reduce(lambda x,y: x+[y] if not y in x else x, l, [])
    if l: m.h['f']['d']['statisticalCodeIds'] = l


def map_holdings_statements(m):
    # Christie and Natasha will work on suppression of non-public notes
    hst, hsti, hsfs, extp = ['holdingsStatements',
                             'holdingsStatementsForIndexes', 
                             'holdingsStatementsForSupplements',
                             'ext_ownership_type_id']
    for s in [hst, hsti, hsfs]: m.h['f']['d'][s] = []
    for d in m.h['o']['r']['ext_ownership']:
        notes = ' '.join(['%s (%s)' % (a['note'], a['type']) for a in d['notes'] \
                          if not (m.public and a['type'] == 'non-public')])
        statement = dict([['statement', d['text']], ['note', notes]])
        if d[extp] == 1: m.h['f']['d'][hst].append(statement)
        elif d[extp] == 2: m.h['f']['d'][hsti].append(statement)
        elif d[extp] == 3: m.h['f']['d'][hsfs].append(statement)


def map_holdings_metadata(m):
    meta = add_metadata(m, created_by = m.h['o']['r']['created_by'],
                        date_created = m.h['o']['r']['date_created'],
                        updated_by = m.h['o']['r']['updated_by'],
                        date_updated = m.h['o']['r']['date_updated'])
    m.h['f']['d']['metadata'] = meta

    
#@timing
def create_holdings_row(m):
    if 'createdByUserId' in m.h['f']['d']['metadata']:
        created_by = m.h['f']['d']['metadata']['createdByUserId']
    else: created_by = None
    m.h['f']['r'] = dict([['_id', m.h['o']['r']['uuid']],
                     ['jsonb', m.h['f']['d']],
                     ['creation_date', loc_to_utc(m.h['o']['r']['date_created'])],
                     ['created_by', created_by],
                     ['instanceid', m.h['f']['d']['instanceId']],
                     ['permanentlocationid', m.h['f']['d']['permanentLocationId']],
                     ['temporarylocationid', None],
                     ['holdingstypeid', m.h['f']['d']['holdingsTypeId']],
                     ['callnumbertypeid', m.h['f']['d']['callNumberTypeId'] \
                      if 'callNumberTypeId' in m.h['f']['d'] else None],
                     ['illpolicyid', m.h['f']['d']['illPolicyId'] \
                      if 'illpolicyId' in m.h['f']['d'] else None],
                     ['bib_id', m.h['o']['r']['bib_id']],
                     ['holdings_id', m.h['o']['r']['holdings_id']]])

    
@timing
def validate_holdings_rows(m):
    if m.validate:
        for h in [h for h in m.hl if h['include']]:
            r = h['f']['r']
            try:
                m.validate_holdings(r['jsonb'])
            except:
                m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], r['holdings_id'], None))
                m.log.error("error in holdings row %s" % r['holdings_id'], exc_info=m.stack_trace)
                h['include'] = False

            
@timing
def format_holdings_rows(m):
    for h in [h for h in m.hl if h['include']]:
        r = h['f']['r']
        try:
            r['jsonb'] = json.dumps(r['jsonb'], default=serialize_datatype)
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], r['holdings_id'], None))
            m.log.error("error in holdings row %s" % r['holdings_id'], exc_info=m.stack_trace)
            h['include'] = False

            
@timing
def save_holdings_rows(m):
    l = [h for h in m.hl if h['include']]
    if l:
        count = len(l)
        cols = m.col['holdings_record']
        sql = "insert into holdings_record values %s"
        rows = [list(h['f']['r'].values())[:-2] for h in l]
        template = '(%s)' % ', '.join(['%s'] * len(cols))
        try:
            execute_values(m.pdb.cur, sql, rows, template=template, page_size=count)
            m.pdb.con.commit()
            m.log.debug("saved holdings rows")
        except:
            m.pdb.con.rollback()
            sql = "insert into holdings_record values (%s)" % ','.join(['%s'] * len(cols))
            for h in l:
                r = h['f']['r']
                try:
                    m.pdb.cur.execute(sql, list(r.values())[:-2])
                    m.pdb.con.commit()
                except:
                    m.pdb.con.rollback()
                    r['jsonb'] = json.loads(r['jsonb'])
                    m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], r['holdings_id'], None))
                    m.log.error("error in holdings r %s" % r['holdings_id'], exc_info=m.stack_trace)
                    m.log.debug(pformat(r))
                    h['include'] = False
        finally:
            m.hct2 = m.hct2 + len([h for h in m.hl if h['include']])

            
##### items


@timing
def fetch_item_rows(m):
    m.il = []
    holdings_ids = [h['o']['r']['holdings_id'] for h in m.hl if h['include']]
    if holdings_ids:
        s = ','.join(['%s'] * len(holdings_ids))
        # l = "','".join(exclude_locations(m))
        cols = m.col['ole_ds_item_t'] + ['bib_id', 'uuid', 'holdings_uuid']
        sql = "select i.*, h.bib_id, u.uuid, v.uuid from ole_ds_item_t i " \
              "join ole_ds_holdings_t h on i.holdings_id = h.holdings_id " \
              "join cat.uuid u on i.item_id = u.id " \
              "join cat.uuid v on h.holdings_id = v.id " \
              "where u.table_name = 'ole_ds_item_t' " \
              "and v.table_name = 'ole_ds_holdings_t' " \
              "and i.holdings_id in (%s) " % s
        m.mdb.cur.execute(sql, holdings_ids)
        m.il = [new_row(dict([[c,v] for c,v in zip(cols,r)])) \
                for r in m.mdb.cur.fetchall()]
        m.id = dict([[i['o']['r']['item_id'], i] for i in m.il])
        m.ict1 = m.ict1 + len(m.il)
        m.log.debug("fetched %d item rows" % len(m.il))


@timing
def fetch_item_former_ids(m):
    if m.il:
        item_ids = [i['o']['r']['item_id'] for i in m.il]
        cols = m.col['ole_ds_itm_former_identifier_t']
        s = ','.join(['%s'] * len(item_ids))
        sql = "select * from ole_ds_itm_former_identifier_t where item_id in (%s)" % s
        m.mdb.cur.execute(sql, item_ids)
        former_id_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for i in m.il: i['o']['r']['former_ids'] = []
        for d in former_id_rows:
            if d['value']: m.id[d['item_id']]['o']['r']['former_ids'].append(d['value'])
        m.log.debug("fetched %d item former_id rows" % len(former_id_rows))


@timing
def fetch_item_note_rows(m):
    if m.il:
        item_ids = [i['o']['r']['item_id'] for i in m.il]
        cols = m.col['ole_ds_item_note_t']
        s = ','.join(['%s'] * len(item_ids))
        sql = "select * from ole_ds_item_note_t where item_id in (%s)" % s
        m.mdb.cur.execute(sql, item_ids)
        note_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for i in m.il: i['o']['r']['notes'] = []
        for d in note_rows:
            if d['note']: m.id[d['item_id']]['o']['r']['notes'].append(d)
        m.log.debug("fetched %d item note rows" % len(note_rows))

    
@timing
def fetch_item_statistical_code_rows(m):
    if m.il:
        item_ids = [i['o']['r']['item_id'] for i in m.il]
        cols = m.col['ole_ds_item_stat_search_t']
        s = ','.join(['%s'] * len(item_ids))
        sql = "select * from ole_ds_item_stat_search_t where item_id in (%s)" % s
        m.mdb.cur.execute(sql, item_ids)
        stat_cd_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for i in m.il: i['o']['r']['statistical_codes'] = []
        for d in stat_cd_rows:
            if d['stat_search_code_id']: m.id[d['item_id']]['o']['r']['statistical_codes'].append(d)
        m.log.debug("fetched %d statistical_codes rows" % len(stat_cd_rows))


@timing
def fetch_item_order_item_rows(m):
    if m.il:
        item_ids = [i['o']['r']['item_id'] for i in m.il]
        cols = ['id', 'uuid']
        s = ','.join(['%s'] * len(item_ids))
        sql = "select id, uuid from cat.uuid where table_name = 'order_item' and id in (%s)" % s
        m.mdb.cur.execute(sql, item_ids)
        order_item_rows = [dict([[c,v] for c,v in zip(cols,r)]) for r in m.mdb.cur.fetchall()]
        for i in m.il: i['o']['r']['order_item_id'] = None
        for d in order_item_rows:
            if d['id']: m.id[d['id']]['o']['r']['order_item_id'] = d['uuid']
        m.log.debug("fetched %d order_item rows" % len(order_item_rows))


@timing
def map_item_rows(m):
    for i in m.il:
        m.i = i
        r = i['o']['r']
        try:
            create_item_doc(m)
            create_item_row(m)
        except:
            ln = '%d\t%d\t%d\n' % (r['bib_id'], r['holdings_id'], r['item_id'])
            m.reject_file.write(ln)
            if i['include']:
                e = 'unable to map item: %d' % r['item_id']
                m.log.error(e, exc_info=m.stack_trace)
                m.log.debug(pformat(r))
                i['include'] = False


#@timing
def create_item_doc(m):
    m.i['f']['d'] = dict()
    map_item_id(m)
    map_item_hrid(m)
    map_item_holdings_id(m)
    map_item_former_ids(m)
    map_item_discovery_suppress(m)
    map_item_accession_number(m)
    map_item_barcode(m)
    map_item_level_call_number(m)
    map_item_level_call_number_prefix(m)
    map_item_level_call_number_suffix(m)
    map_item_level_call_number_type(m)
    map_item_effective_call_number_components(m)
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
    map_item_status(m)
    map_item_material_type(m)
    map_item_permanent_loan_type(m)
    map_item_temporary_loan_type(m)
    map_item_permanent_location(m)
    map_item_temporary_location(m)
    map_item_effective_location(m)
    map_item_electronic_access(m)
    map_item_intransit_service_point(m)
    map_item_statistical_codes(m)
    map_item_order_id(m)
    map_item_metadata(m)

    
def map_item_id(m):
    m.i['f']['d']['id'] = m.i['o']['r']['uuid']


def map_item_hrid(m):
    if m.i['o']['r']['item_id']:
        m.i['f']['d']['hrid'] = 'it' + str(m.i['o']['r']['item_id'])


def map_item_holdings_id(m):
    m.i['f']['d']['holdingsRecordId'] = m.i['o']['r']['holdings_uuid']


def map_item_former_ids(m):
    l =  m.i['o']['r']['former_ids']
    l = [str(id) for id in l]
    l = reduce(lambda x,y: x+[y] if not y in x else x, l, [])
    if l: m.i['f']['d']['formerIds'] = l


def map_item_discovery_suppress(m):
    staff_only = {'Y':True, 'N':False}[m.i['o']['r']['staff_only']]
    if m.public and staff_only:
        m.i['include'] = False
        e = "Item id %s is not for public use" % m.i['o']['r']['item_id']
        raise Exception (e)
    if staff_only: m.i['f']['d']['discoverySuppress'] = staff_only


def map_item_accession_number(m):
    # Do not use
    # v = m.i['f']['d']['accessionNumber']
    # if v: m.i['f']['d']['accessionNumber'] = v
    pass

def map_item_barcode(m): 
    v = m.i['o']['r']['barcode']
    if v: m.i['f']['d']['barcode'] = v


def map_item_level_call_number(m):
    v = m.i['o']['r']['call_number']
    if v: m.i['f']['d']['itemLevelCallNumber'] = v


def map_item_level_call_number_prefix(m):
    v = m.i['o']['r']['call_number_prefix']
    if v: m.i['f']['d']['itemLevelCallNumberPrefix'] = v


def map_item_level_call_number_suffix(m):
    # m.i['f']['d']['itemLevelCallNumberSuffix'] = None
    pass


def map_item_effective_call_number_components(m):
    j = dict()
    hid = m.i['o']['r']['holdings_id']
    if 'itemLevelCallNumber' in m.i['f']['d']:
        j['callNumber'] = m.i['f']['d']['itemLevelCallNumber']
        if 'itemLevelCallNumberPrefix' in m.i['f']['d']:
            j['prefix'] = m.i['f']['d']['itemLevelCallNumberPrefix']
        if 'itemLevelCallNumberSuffix' in m.i['f']['d']:
            j['suffix'] = m.i['f']['d']['itemLevelCallNumberSuffix']
    elif 'callNumber' in m.hd[hid]['f']['d']:
        j['callNumber'] = m.hd[hid]['f']['d']['callNumber']
        if 'callNumberPrefix' in m.hd[hid]['f']['d']:
            j['prefix'] = m.hd[hid]['f']['d']['callNumberPrefix']
        if 'callN;numberSuffix' in m.hd[hid]['f']['d']:
            j['suffix'] = m.hd[hid]['f']['d']['callNumberSuffix']
    if j: m.i['f']['d']['effectiveCallNumberComponents'] = j


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
    v = m.i['o']['r']['call_number_type_id']
    if v in d:
        m.i['f']['d']['itemLevelCallNumberTypeId'] = m.ref['call_number_type'][d[v]]


def map_item_volume(m):
    # m.i['f']['d']['volume'] = None
    pass


def map_item_enumeration(m):
    v = m.i['o']['r']['enumeration']
    if v: m.i['f']['d']['enumeration'] = v


def map_item_chronology(m):
    v = m.i['o']['r']['chronology']
    if v: m.i['f']['d']['chronology'] = v


def map_item_year_caption(m):
    # m.i['f']['d']['yearCaption'] = []
    pass


def map_item_identifier(m):
    # m.i['f']['d']['itemIdentifier'] = None
    pass


def map_item_copy_numbers(m):
    v = m.i['o']['r']['copy_number']
    if v: m.i['f']['d']['copyNumbers'] = [str(v)]


def map_item_num_of_pieces(m):
    v = m.i['o']['r']['num_pieces']
    if re.match(m.int_pat, str(v)) and int(v) > 1: v = v
    else: v = None
    if v: m.i['f']['d']['numberOfPieces'] = v


def map_item_description_of_pieces(m):
    v = m.i['o']['r']['desc_of_pieces']
    if v: m.i['f']['d']['descriptionOfPieces'] = v


def map_item_number_of_missing_pieces(m):
    # skip for now; current values not working with loans
    # These missing properties should be their own repeating json object.
    pass
    # v = m.i['o']['r']['missing_pieces_count']
    # v = str(v) if isinstance(v, int) else None
    # if v: m.i['f']['d']['numberOfMissingPieces'] = v


def map_item_missing_pieces(m):
    # skip for now; current values not working with loans
    # These missing properties should be their own repeating json object.
    # v = m.i['o']['r']['missing_pieces']
    # if v: m.i['f']['d']['missingPieces'] = v
    pass


def map_item_missing_pieces_date(m):
    # skip for now; current values not working with loans
    # These missing properties should be their own repeating json object.
    # v = m.i['o']['r']['missing_pieces_effective_date']
    # v = date_to_str(v)
    # if v: m.i['f']['d']['missingPiecesDate'] = v
    pass


def map_item_item_damaged_status_id(m):
    # There should be a lookup table for this
    # m.i['f']['d']['itemDamagedStatusId'] = None
    pass


def map_item_item_damaged_status_date(m):
    # m.i['f']['d']['itemDamagedStatusDate'] = None
    pass


def map_item_notes(m):
    # Currently only using stock Folio item_note_type entries
    # Fix note type to pull from ref table.
    acc = []
    for d in m.i['o']['r']['notes']:
        if not (m.public and d['type'] == 'nonPublic'):
            # j = dict([['itemNoteTypeId', '8d0a5eca-25de-4391-81a9-236eeefdd20b'],
            j = dict([['itemNoteTypeId', m.ref['item_note_type']['Note']],
                      ['note', d['note']],
                      ['staffOnly', True if d['type'] == 'nonPublic' else False]])
            acc.append(j)
    if acc: m.i['f']['d']['notes'] = acc
    
    
def map_item_circulation_notes(m):
    # Note yet implemented
    # Check with Cheryl about how to use note types
    keys = ['check_in_note', 'claims_returned_note']
    acc = []
    for n in [m.i['o']['r'][k] for k in keys]:
        if n not in [None, ' ', '']:
            j = dict([['noteType', 'check-in'],
                      ['note', n],
                      ['staffOnly', True]])
            acc.append(j)
    if acc and not m.public: m.i['f']['d']['circulationNotes'] = acc
    
    
def map_item_status(m):
    # Talk to Cheryl and David about this 
    # For now, code everything as 'AVAILABLE'
    # Status name cannot be null
    d = dict([(1, 'ANAL'),
              (2, 'AVAILABLE'),
              (4, 'AVAILABLE-AT-MANSUETO'),
              (5,'INPROCESS'),
              (6, 'INPROCESS-CRERAR'),
              (7, 'INPROCESS-LAW'),
              (8, 'INPROCESS-MANSUETO'),
              (9, 'INPROCESS-REGENSTEIN'),
              (10, 'INTRANSIT'),
              (11, 'INTRANSIT-FOR-HOLD'),
              (12, 'INTRANSIT-PER-STAFF-REQUEST'),
              (13, 'LOANED'),
              (14, 'LOST'),
              (15, 'MISSING'),
              (16, 'MISSING-FROM-MANSUETO'),
              (18, 'ONORDER'),
              (19, 'RECENTLY-RETURNED'),
              (20, 'RETRIEVING-FROM-MANSUETO'),
              (21, 'RETURNED-DAMAGED'),
              (22, 'RETURNED-WITH-MISSING-ITEMS'),
              (23, 'UNAVAILABLE'),
              (25, 'DECLARED-LOST'),
              (24, 'FLAGGED-FOR-RESERVE'),
              (26, 'WITHDRAWN'),
              (27, 'On Order'), (17, 'ONHOLD'),
              (28, 'In Process'),
              (32,'LOST-AND-PAID'),
              (35, 'BTAASPR'),
              (36, 'WITHDRAWN-SPR-BTAA')])
    id = m.i['o']['r']['item_status_id']
    dt = date_to_str(m.i['o']['r']['item_status_date_updated'])
    # j = dict([['name', 'AVAILABLE'], ['date', dt]]) if id in d else None
    j = dict([['name', 'AVAILABLE'], ['date', dt]])
    if j: m.i['f']['d']['status'] = j
    

def map_item_material_type(m):
    # This is a required field
    m.i['f']['d']['materialTypeId'] = m.ref['material_type']['unspecified']


def map_item_permanent_loan_type(m):
    # This is a required field
    # Talk to Cheryl and David about this 
    cols = ['id', 'name']
    d = dict([[1, 'stks'],[2, 'buo'],[3, 'mus'],[4, 'res2'],[5, 'spcl'],[6, '16mmcam'],
              [7, 'AVSadpt'],[8, 'AVSasis'],[9, 'AVSmic'],[10, 'AVSport'],[11, 'AVSproj'],
              [12, 'AVSscre'],[13, 'batt'],[14, 'boompol'],[15, 'bordirc'],[16, 'cabl'],
              [17, 'camrig'],[18, 'dslr'],[19, 'dslracc'],[20, 'eres'],[21, 'games'],
              [22, 'gaming'],[23, 'grip'],[24, 'hdcam'],[25, 'headph'],[26, 'ilok'],
              [27, 'inhouse'],[28, 'its2adp'],[29, 'its8adp'],[30, 'its8cnf'],[31, 'its8ipd'],
              [32, 'its8lap'],[33, 'lghtmtr'],[34, 'lights'],[35, 'lmc6wk'],[36, 'macadpt'],
              [37, 'mics'],[38, 'micstnd'],[39, 'mixer'],[40, 'monitrs'],[41, 'noncirc'],
              [42, 'online'],[43, 'playbck'],[44, 'projctr'],[45, 'res168'],[46, 'res24'],
              [47, 'res4'],[48, 'res48'],[49, 'res72'],[50, 'sndacc'],[51, 'sndrec'],
              [52, 'spkrs'],[53, 'stilcam'],[54, 'stks14'],[55, 'stks7'],[56, 'tripod'],
              [57, 'vidcam'],[58, 'yerk'],[182, 'illbuo'],[183, 'ill7day'],[184, 'ill7dayrenew'],
              [185, 'ill21day'],[186, 'ill21dayrenew'],[187, 'ill42day'],[188, 'ill42dayrenew'],
              [189, 'ill77day'],[190, 'ill77dayrenew'],[281, 'itspol2adp'],[282, 'itspol8adp'],
              [283, 'itspol8vid'],[284, 'itspol8ipd'],[285, 'itspol8lap']])
    v = m.ref['loan_type'][d[m.i['o']['r']['item_type_id']]] \
        if m.i['o']['r']['item_type_id'] in d else m.ref['loan_type']['stks']
    m.i['f']['d']['permanentLoanTypeId'] = v

    
def map_item_temporary_loan_type(m):
    # Talk to Cheryl and David about this 
    # Required db column value
    cols = ['id', 'name']
    d = dict([[1, 'stks'],[2, 'buo'],[3, 'mus'],[4, 'res2'],[5, 'spcl'],[6, '16mmcam'],
              [7, 'AVSadpt'],[8, 'AVSasis'],[9, 'AVSmic'],[10, 'AVSport'],[11, 'AVSproj'],
              [12, 'AVSscre'],[13, 'batt'],[14, 'boompol'],[15, 'bordirc'],[16, 'cabl'],
              [17, 'camrig'],[18, 'dslr'],[19, 'dslracc'],[20, 'eres'],[21, 'games'],
              [22, 'gaming'],[23, 'grip'],[24, 'hdcam'],[25, 'headph'],[26, 'ilok'],
              [27, 'inhouse'],[28, 'its2adp'],[29, 'its8adp'],[30, 'its8cnf'],[31, 'its8ipd'],
              [32, 'its8lap'],[33, 'lghtmtr'],[34, 'lights'],[35, 'lmc6wk'],[36, 'macadpt'],
              [37, 'mics'],[38, 'micstnd'],[39, 'mixer'],[40, 'monitrs'],[41, 'noncirc'],
              [42, 'online'],[43, 'playbck'],[44, 'projctr'],[45, 'res168'],[46, 'res24'],
              [47, 'res4'],[48, 'res48'],[49, 'res72'],[50, 'sndacc'],[51, 'sndrec'],
              [52, 'spkrs'],[53, 'stilcam'],[54, 'stks14'],[55, 'stks7'],[56, 'tripod'],
              [57, 'vidcam'],[58, 'yerk'],[182, 'illbuo'],[183, 'ill7day'],[184, 'ill7dayrenew'],
              [185, 'ill21day'],[186, 'ill21dayrenew'],[187, 'ill42day'],[188, 'ill42dayrenew'],
              [189, 'ill77day'],[190, 'ill77dayrenew'],[281, 'itspol2adp'],[282, 'itspol8adp'],
              [283, 'itspol8vid'],[284, 'itspol8ipd'],[285, 'itspol8lap']])
    v = m.ref['loan_type'][d[m.i['o']['r']['temp_item_type_id']]] \
        if m.i['o']['r']['temp_item_type_id'] in d else None
    if v: m.i['f']['d']['temporaryLoanTypeId'] = v


def map_item_permanent_location(m):
    d = {'ASR':1,'JRL':2,'JCL':3,'SPCL':4,'ITS':5,'Online':6,'UCX':7,
         'AANet':8,'Eck':9,'SSAd':10,'DLL':11,'MCS':12,'POLSKY':13,
         'LMC':14,'unk':15}
    s = m.i['o']['r']['location']
    if s and len(s.split('/')) == 3:
        ins, lib, shv = s.split('/')
        loc = 'UC/HP/%s/%s' % (lib, shv)
        if loc in m.ref['location']:
            m.i['f']['d']['permanentLocationId'] = m.ref['location'][loc] 


def map_item_temporary_location(m):
    # Required db column
    # m.i['f']['d']['temporaryLocationId'] = None
    pass


def map_item_effective_location(m):
    pid = 'permanentLocationId'
    hid = m.i['o']['r']['holdings_id']
    if pid in m.i['f']['d']:
        m.i['f']['d']['effectiveLocationId'] = m.i['f']['d'][pid]
    elif pid in m.hd[hid]['f']['d']:
        m.i['f']['d']['effectiveLocationId'] = m.hd[hid]['f']['d'][pid]
        
    
def map_item_electronic_access(m):
    # m.i['f']['d']['electronicAccess'] = None
    pass


def map_item_intransit_service_point(m):
    # Cheryl says to remove this id from inventory
    item_loc =  m.i['o']['r']['location']
    holdings_loc =  m.hd[m.i['o']['r']['holdings_id']]['o']['r']['location']
    if item_loc and len(item_loc.split('/')) == 3: _,lib,_ = item_loc.split('/')
    elif holdings_loc and holdings_loc.split('/') == 3: _,lib,_ = holdings_loc.split('/')
    else: lib = 'JRL'
    # m.i['f']['d']['inTransitDestinationServicePointId'] = m.ref['service_point'][lib]


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
    l = [d[a['stat_search_code_id']] for a in m.i['o']['r']['statistical_codes']]
    l = [m.ref['statistical_code'][a] for a in l]
    l = reduce(lambda x,y: x+[y] if not y in x else x, l, [])
    if l: m.i['f']['d']['statisticalCodeIds'] = l


def map_item_order_id(m):
    if not m.public:
        if m.i['o']['r']['order_item_id']:
            uuid = m.i['o']['r']['order_item_id']
            m.i['f']['d']['purchaseOrderLineIdentifier'] = uuid


def map_item_metadata(m):
    m.i['f']['d']['metadata'] = add_metadata(m, created_by = m.i['o']['r']['created_by'],
                                         date_created = m.i['o']['r']['date_created'],
                                         updated_by = m.i['o']['r']['created_by'],
                                         date_updated = m.i['o']['r']['date_updated'])

    
#@timing
def create_item_row(m):
    if 'createdByUserId' in m.i['f']['d']['metadata']:
        created_by = m.i['f']['d']['metadata']['createdByUserId']
    else: created_by = None
    m.i['f']['r'] = dict([['_id', m.i['o']['r']['uuid']],
                     ['jsonb', m.i['f']['d']],
                     ['creation_date', loc_to_utc(m.i['o']['r']['date_created'])],
                     ['created_by', created_by],
                     ['holdingsrecordid', m.i['o']['r']['holdings_uuid']],
                     ['permanentloantypeid', m.i['f']['d']['permanentLoanTypeId']],
                     ['temporaryloantypeid', m.i['f']['d']['temporaryLoanTypeId'] \
                      if 'temporaryLoanTypeId' in m.i['f']['d'] else None],
                     ['materialtypeid', m.i['f']['d']['materialTypeId']],
                     ['permanentlocationid', m.i['f']['d']['permanentLocationId'] \
                      if 'permanentlocationid' in m.i['f']['d'] else None],
                     ['temporarylocationid', None],
                     ['effectivelocationid', m.i['f']['d']['effectiveLocationId'] \
                      if 'effectivelocationid' in m.i['f']['d'] else None],
                     ['item_id', m.i['o']['r']['item_id']],
                     ['holdings_id', m.i['o']['r']['holdings_id']],
                     ['bib_id', m.i['o']['r']['bib_id']]])


@timing
def validate_item_rows(m):
    if m.validate:
        for i in [i for i in m.il if i['include']]:
            r = i['f']['r']
            try:
                m.validate_item(r['jsonb'])
            except:
                m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], r['holdings_id'], r['item_id']))
                m.log.error("error in item row %s" % r['item_id'], exc_info=m.stack_trace)
                i['include'] = False

            
@timing
def format_item_rows(m):
    for i in [i for i in m.il if i['include']]:
        r = i['f']['r']
        try:
            r['jsonb'] = json.dumps(r['jsonb'], default=serialize_datatype)
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], r['holdings_id'], r['item_id']))
            m.log.error("error in item row %s" % r['item_id'], exc_info=m.stack_trace)
            i['include'] = False

            
@timing
def save_item_rows(m):
    l = [i for i in m.il if i['include']]
    if l:
        count = len(l)
        cols = m.col['item']
        sql = "insert into item values %s"
        rows = [list(i['f']['r'].values())[:-3] for i in l]
        template = '(%s)' % ', '.join(['%s'] * len(cols))
        try:
            execute_values(m.pdb.cur, sql, rows, template=template, page_size=count)
            m.pdb.con.commit()
            m.log.debug("saved item rows")
        except:
            m.pdb.con.rollback()
            sql = "insert into item values (%s)" % ','.join(['%s'] * len(cols))
            for i in l:
                r = i['f']['r']
                try:
                    m.pdb.cur.execute(sql, list(r.values())[:-3])
                    m.pdb.con.commit()
                except:
                    m.pdb.con.rollback()
                    r['jsonb'] = json.loads(r['jsonb'])
                    m.reject_file.write('%s\t%s\t%s\n'% (r['bib_id'],r['holdings_id'],r['item_id']))
                    m.log.error("error in item r %s" % r['item_id'], exc_info=m.stack_trace)
                    m.log.debug(pformat(r))
                    i['include'] = False
        finally:
            m.ict2 = m.ict2 + len([i for i in m.il if i['include']])


##### reference
            

def load_reference_data(m):
    load_ref_uuids(m)
    delete_ref_tables(m)
    load_users(m)
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
    load_ill_policy(m)
    load_instance_note_type(m)
    load_holdings_note_type(m)
    load_item_note_type(m)


def delete_ref_tables(m):
    if m.save_ref_tables:
        for t in [d['tbl'] for d in m.tables if d['type'] == 'ref']:
            sql = f"delete from {t};"
            m.pdb.cur.execute(sql)
            m.pdb.con.commit()


def load_ref_uuids(m):
    for t in [d['tbl'] for d in m.tables if d['type'] == 'ref']:
        m.uuid[t] = dict(fetch_uuids(m, t))


def fetch_uuids(m, table_name):
    sql = f"select id, uuid from cat.uuid where table_name = '{table_name}'"
    m.mdb.cur.execute(sql)
    return m.mdb.cur.fetchall()


def load_users(m):
    sql = "select jsonb from users"
    m.pdb.cur.execute(sql)
    l = [d for l in m.pdb.cur.fetchall() for d in l]
    m.pdb.con.commit()
    m.ref['user'] = dict([[d['username'],d['id']] for d in l if 'username' in d])
    m.ref_dict['user'] = dict([[d['username'],d] for d in l if 'username' in d])
    if not m.public and m.cur_user in m.ref['user']:
        m.cur_user_id = m.ref['user'][m.cur_user]
    else: m.cur_user_id = None

    
def load_alternative_title_type(m):
    sql = "delete from alternative_title_type"
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()
    tbl = 'alternative_title_type'
    cols = ['id','name', 'source']
    rows = [[1, 'Uniform Title', 'local'],
            [2, 'Variant Title', 'local'],
            [3, 'Former Title', 'local']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_instance_note_type(m):
    sql = "delete from instance_note_type"
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()
    tbl = 'instance_note_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'General note', 'local'],
            [2, 'With note', 'local'],
            [3, 'Dissertation note', 'local'],
            [4, 'Bibliography note', 'local'],
            [5, 'Formatted Contents Note', 'local'],
            [6, 'Restrictions on Access note', 'local'],
            [7, 'Scale note for graphic material', 'local'],
            [8, 'Creation / Production Credits note', 'local'],
            [9, 'Citation / References note', 'local'],
            [10, 'Participant or Performer note', 'local'],
            [11, 'Type of report and period covered note', 'local'],
            [12, 'Data quality note', 'local'],
            [13, 'Numbering peculiarities note', 'local'],
            [14, 'Type of computer file or data note', 'local'],
            [15, 'Date / time and place of an event note', 'local'],
            [16, 'Summary', 'local'],
            [17, 'Geographic Coverage note', 'local'],
            [18, 'Preferred Citation of Described Materials note', 'local'],
            [19, 'Supplement note', 'local'],
            [20, 'Additional Physical Form Available note', 'local'],
            [21, 'Accessibility note', 'local'],
            [22, 'Reproduction note', 'local'],
            [23, 'Original Version note', 'local'],
            [24, 'Terms Governing Use and Reproduction note', 'local'],
            [25, 'Immediate Source of Acquisition note', 'local'],
            [26, 'Information related to Copyright Status', 'local'],
            [27, 'Location of Other Archival Materials note', 'local'],
            [28, 'Biographical or Historical Data', 'local'],
            [29, 'Language note', 'local'],
            [30, 'Former Title Complexity note', 'local'],
            [31, 'Issuing Body note', 'local'],
            [32, 'Entity and Attribute Information note', 'local'],
            [33, 'Cumulative Index / Finding Aides notes', 'local'],
            [34, 'Information About Documentation note', 'local'],
            [35, 'Ownership and Custodial History note', 'local'],
            [36, 'Copy and Version Identification note', 'local'],
            [37, 'Binding Information note', 'local'],
            [38, 'Case File Characteristics note', 'local'],
            [39, 'Methodology note', 'local'],
            [40, 'Linking Entry Complexity note', 'local'],
            [41, 'Action note', 'local'],
            [42, 'Awards note', 'local'],
            [43, 'Local notes', 'local']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_identifier_type(m):
    tbl = 'identifier_type'
    cols = ['id','name']
    rows = [[1, 'OCLC'],
            [2, 'Cancelled OCLC'],
            [3, 'LCCN'],
            [4, 'Invalid LCCN'],
            [5, 'ISBN'],
            [6, 'Invalid ISBN'],
            [7, 'ISSN'],
            [8, 'Invalid ISSN'],
            [9, 'Linking ISSN'],
            [10, 'Other Standard Identifier'],
            [11, 'Cancelled Other Standard Identifier'],
            [12, 'Publisher or Distributor Number'],
            [13, 'Cancelled Publisher or Distributor Number'],
            [14, 'System Control Number'],
            [15, 'Cancelled System Control Number'],
            [16, 'GPO Item Number'],
            [17, 'Cancelled GPO Item Number']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_contributor_name_type(m):
    tbl = 'contributor_name_type'
    cols = ['id','name']
    rows = [[1, 'Personal name'],
            [2, 'Corporate name'],
            [3, 'Meeting name']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


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
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables:
        cols = ['_id','jsonb']
        rows = [[d['id'], json.dumps(d)] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.con.commit()


def load_classification_type(m):
    tbl = 'classification_type'
    cols = ['id','name']
    rows = [[1, 'Library of Congress Classification'],
            [2, 'Dewey Decimal Classification'],
            [3, 'Government Document Classification'],
            [4, 'Library of Congress Classification (Local)']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_ill_policy(m):
    tbl = 'ill_policy'
    cols = ['id', 'name', 'source']
    rows = [[1, 'undefined', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


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
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.save_ref_tables:
        cols = ['_id','jsonb']
        rows = [[d['id'], json.dumps(d)] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.con.commit()


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
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.save_ref_tables:
        cols = ['_id','jsonb']
        rows = [[d['id'], json.dumps(d)] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.con.commit()


def load_mode_of_issuance(m):
    tbl = 'mode_of_issuance'
    cols = ['id','name']
    rows = [[1, 'Monographic component part'],
            [2, 'Serial component part'],
            [3, 'Collection'],
            [4, 'Subunit'],
            [5, 'Integrating resource'],
            [6, 'Monograph'],
            [7, 'Serial']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_instance_status(m):
    # Todo Talk to Christie about new categories to add
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
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_call_number_type(m):
    tbl = 'call_number_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'No information provided', 'local'],
            [2, 'Library of Congress classification', 'local'],
            [3, 'Dewey Decimal classification', 'local'],
            [4, 'National Library of Medicine classification', 'local'],
            [5, 'Superintendent of Documents classification', 'local'],
            [6, 'Shelving control number', 'local'],
            [7, 'Title', 'local'],
            [8, 'Shelved separately', 'local'],
            [9, 'Source specified in subfield $2', 'local'],
            [10, 'Other scheme', 'local']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_holdings_type(m):
    tbl = 'holdings_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'physical', 'UC'],
            [2, 'electronic', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_holdings_note_type(m):
    sql = "delete from holdings_note_type"
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()
    tbl = 'holdings_note_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'Electronic bookplate', 'folio'],
            [2, 'Binding', 'folio'],
            [3, 'Reproduction', 'folio'],
            [4, 'Note', 'folio'],
            [5, 'Action note', 'folio'],
            [6, 'Provenance', 'folio'],
            [7, 'Copy note', 'folio']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_item_note_type(m):
    sql = "delete from item_note_type"
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()
    tbl = 'item_note_type'
    cols = ['id', 'name', 'source']
    rows = [[1, 'Electronic bookplate', 'folio'],
            [2, 'Binding', 'folio'],
            [3, 'Reproduction', 'folio'],
            [4, 'Note', 'folio'],
            [5, 'Action note', 'folio'],
            [6, 'Provenance', 'folio'],
            [7, 'Copy note', 'folio']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


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
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


@timing
def load_loan_types(m):
    tbl = 'loan_type'
    cols = ['id', 'name']
    rows = [[1, 'stks'],[2, 'buo'],[3, 'mus'],[4, 'res2'],[5, 'spcl'],[6, '16mmcam'],
            [7, 'AVSadpt'],[8, 'AVSasis'],[9, 'AVSmic'],[10, 'AVSport'],[11, 'AVSproj'],
            [12, 'AVSscre'],[13, 'batt'],[14, 'boompol'],[15, 'bordirc'],[16, 'cabl'],
            [17, 'camrig'],[18, 'dslr'],[19, 'dslracc'],[20, 'eres'],[21, 'games'],
            [22, 'gaming'],[23, 'grip'],[24, 'hdcam'],[25, 'headph'],[26, 'ilok'],
            [27, 'inhouse'],[28, 'its2adp'],[29, 'its8adp'],[30, 'its8cnf'],[31, 'its8ipd'],
            [32, 'its8lap'],[33, 'lghtmtr'],[34, 'lights'],[35, 'lmc6wk'],[36, 'macadpt'],
            [37, 'mics'],[38, 'micstnd'],[39, 'mixer'],[40, 'monitrs'],[41, 'noncirc'],
            [42, 'online'],[43, 'playbck'],[44, 'projctr'],[45, 'res168'],[46, 'res24'],
            [47, 'res4'],[48, 'res48'],[49, 'res72'],[50, 'sndacc'],[51, 'sndrec'],
            [52, 'spkrs'],[53, 'stilcam'],[54, 'stks14'],[55, 'stks7'],[56, 'tripod'],
            [57, 'vidcam'],[58, 'yerk'],[59, 'illbuo'],[60, 'ill7day'],[61, 'ill7dayrenew'],
            [62, 'ill21day'],[63, 'ill21dayrenew'],[64, 'ill42day'],[65, 'ill42dayrenew'],
            [66, 'ill77day'],[67, 'ill77dayrenew'],[68, 'itspol2adp'],[69, 'itspol8adp'],
            [70, 'itspol8vid'],[71, 'itspol8ipd'],[72, 'itspol8lap']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)

        
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
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['name'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['name'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)


def load_statistical_code_type(m):
    # Change UUID count
    # Used by statistical_code
    tbl = 'statistical_code_type'
    cols = ['id','name', 'source']
    rows = [[1, 'University of Chicago', 'UC'],
            [2, 'Serial status', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['id'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['id'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)

        
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
            [23, 'polsky', 'Polsky TECHB@R(polsky)', 'UC', 1],
            [24, 'eintegrating', 'E-integrating resource', 'UC', 1],
            [25, 'ASER', 'Active serial', 'Serial status', 2],
            [26, 'ISER', 'Inactive serial', 'Serial status', 2],
            [27, 'ESER', 'Electronic serial', 'Serial status', 2]]
            
    l = [dict(zip(cols, r)) for r in rows]
    for d in l:
        d['id'] = m.uuid[tbl][d['id']]
        d['statisticalCodeTypeId'] = m.uuid['statistical_code_type'][d['statisticalCodeTypeId']]
        d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.save_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by', 'statisticalcodetypeid']
        rows = [[d['id'], json.dumps(d), date_to_str(m.cur_time),
                 m.cur_user_id, d['statisticalCodeTypeId']] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.con.commit()


def load_locinstitution(m):
    tbl = 'locinstitution'
    cols = ['id','name', 'code']
    rows = [[1, 'University of Chicago (UC)', 'UC']]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.save_ref_tables: save_ref_tables(m, tbl, l)

            
def load_loccampus(m):
    tbl = 'loccampus'
    cols = ['id','name', 'code', 'institutionId']
    rows = [[1, 'Hyde Park (HP)', 'HP', 1]]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l: d['id'] = m.uuid[tbl][d['id']]
    for d in l: d['institutionId'] = m.uuid['locinstitution'][d['institutionId']]
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.save_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by', 'institutionid']
        rows = [[d['id'], json.dumps(d), date_to_str(m.cur_time), m.cur_user_id, d['institutionId']] \
                for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.con.commit()


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
    for d in l: d['metadata'] = add_metadata(m)
    m.ref[tbl] = dict([[d['code'], d['id']] for d in l])
    m.ref_dict[tbl] = dict([[d['code'], d] for d in l])
    if m.save_ref_tables:
        cols = ['_id','jsonb', 'creation_date,', 'created_by', 'campusid'] 
        rows = [[d['id'], json.dumps(d), date_to_str(m.cur_time), m.cur_user_id, \
                 d['campusId']] for d in l]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.con.commit()

        
def load_locations(m):
    # There are a few thousand holdings and item locations that are not in this list,
    # Christie is fixing them. The inactive locations were also changed to active.
    # Per Cheryl, used primary service point as only entry in service points list
    tbl = 'location'
    m.ref[tbl] = dict()
    m.ref_dict[tbl] = dict()
    lib = {'ASR': 1,'JRL': 2,'JCL': 3,'SPCL': 4,'ITS': 5,'Online': 6,'UCX': 7,
           'AANet': 8,'Eck': 9,'SSAd': 10,'DLL': 11,'MCS': 12,'POLSKY': 13,
           'LMC': 14,'unk': 15}
    # sps = ['ASR','JRL','JCL','Eck','SSAd','DLL']
    # lib_to_sp = {'ASR':'ASR', 'JRL':'JRL', 'JCL':'JCL', 'SPCL':'JRL', 'ITS':'JRL',
    #              'Online':'JRL', 'UCX':'JRL', 'AANet':'JRL', 'Eck':'Eck', 'SSAd':'SSAd',
    #              'DLL':'DLL', 'MCS':'JRL', 'POLSKY':'JRL', 'LMC':'JRL', 'unk':'JRL'}
    acc = []
    cols = ['id','code','name','libraryId','isActive','campusId','institutionId']
    sql = "select c.locn_cd, c.locn_name, b.locn_cd from ole_locn_t a join ole_locn_t b " \
          "on a.locn_id = b.parent_locn_id join ole_locn_t c on b.locn_id = c.parent_locn_id " \
          "where a.level_id = 1;"
    m.mdb.cur.execute(sql)
    rows = [[i] + list(r) + [True, 1, 1] for i,r in zip(range(1,500),m.mdb.cur.fetchall())]
    l = [dict(zip(cols, r)) for r in rows]
    for d in l:
        j = dict()
        j['id'] = m.uuid[tbl][d['id']]
        j['code'] = 'UC/HP/%s/%s' % (d['libraryId'],d['code'])
        j['description'] = d['name']
        j['discoveryDisplayName'] = d['name']
        j['name'] = j['code']
        j['isActive'] = True
        j['libraryId'] = m.uuid['loclibrary'][lib[d['libraryId']]]
        j['campusId'] = m.uuid['loccampus'][d['campusId']]
        j['institutionId'] = m.uuid['locinstitution'][d['institutionId']]
        # j['primaryServicePoint'] = m.ref['service_point'][lib_to_sp[d['libraryId']]]
        # j['servicePointIds'] = [m.ref['service_point'][lib_to_sp[d['libraryId']]]]
        j['primaryServicePoint'] = m.ref['service_point'][d['libraryId']]
        j['servicePointIds'] = [m.ref['service_point'][d['libraryId']]]
        j['metadata'] = add_metadata(m)
        m.ref[tbl][j['code']] = j['id']
        m.ref_dict[tbl][j['code']] = j
        acc.append(j)
    if m.save_ref_tables:
        cols = ['_id','jsonb','creation_date,','created_by','institutionid','campusid','libraryid']
        rows = [[j['id'], json.dumps(j), date_to_str(m.cur_time), m.cur_user_id, \
                 j['institutionId'], j['campusId'], j['libraryId']] for j in acc]
        execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
        m.pdb.con.commit()

        
def load_service_points(m):
    # service_points are populated by Jon
    tbl = 'service_point'
    lib = {'ASR':'MANSUETO','JRL':'JRLMAIN','JCL':'CRERAR','SPCL':'SCRC',
          'ITS':'ITS','Online':'JRLMAIN','UCX':'JRLMAIN','AANet':'JRLMAIN',
          'Eck':'ECKHART','SSAd':'SSAd','DLL':'LAW','MCS':'JRLMAIN',
          'POLSKY':'POLSKY','LMC':'JRLMAIN','unk':'JRLMAIN'}
    m.pdb.cur.execute("select jsonb from %s" % tbl)
    sp = dict([[d['code'],d] for l in m.pdb.cur.fetchall() for d in l])
    m.ref[tbl] = dict([[k, sp[lib[k]]['id']] for k in lib.keys()])
    m.ref_dict[tbl] = dict([[k, sp[lib[k]]] for k in lib.keys()])


def save_ref_tables(m, tbl, l):
    cols = ['_id','jsonb', 'creation_date,', 'created_by']
    rows = [[d['id'], json.dumps(d), m.cur_time, m.cur_user_id] for d in l]
    execute_values(m.pdb.cur, f"insert into {tbl} values %s;", rows, page_size=len(rows))
    m.pdb.con.commit()


##### validate


# @timing
# def fetch_json_schemas(m):
#     # Fetch the json schemas from github and store them locally
#     if m.fetch_latest_json_schemas:
#         l = [{'repo': 'mod-inventory-storage',
#               'repo_schema': 'mod-inventory-storage/ramls',
#               'local_schema': 'schemas'},
#              {'repo': 'raml',
#               'repo_schema': 'raml/schemas',
#               'local_schema': 'schemas/raml-util/schemas'},
#              {'repo': 'data-import-raml-storage',
#               'repo_schema': 'data-import-raml-storage/schemas/mod-source-record-storage',
#               'local_schema': 'schemas'}
#              # {'repo': 'data-import-raml-storage',
#              #  'repo_schema': 'data-import-raml-storage/schemas/mod-source-record-manager',
#              #  'local_schema': 'schemas'}
#         ]
#         for d in l:
#             local_schema_path = m.cur_path/d['local_schema']
#             local_schema_path.mkdir(exist_ok=True, parents=True)
#             [p.unlink() for p in local_schema_path.iterdir() if p.suffix in ['.schema', '.json']]
#         for d in l:
#             repo_path = m.tmp_path/d['repo']
#             shutil.rmtree(repo_path, ignore_errors=True)
#             cmd = f"git clone https://github.com/folio-org/{d['repo']} {repo_path}"
#             run(cmd.split(), check=True)
#             repo_schema_path = m.tmp_path/d['repo_schema']
#             [shutil.copy(p, local_schema_path) for p in repo_schema_path.iterdir() \
#              if p.suffix in ['.schema', '.json']]
#             [p.rename(str(p.parent/'time_period.json')) for p in local_schema_path.iterdir() \
#              if p.name in ['time-period.json']]
#             shutil.rmtree(repo_path, ignore_errors=True)


# @timing
# def fetch_json_schemas(m):
#     # Fetch the json schemas from github and store them locally
#     if not m.fetch_latest_json_schemas: return
#     temp_dir = '/tmp/temp2/'
#     base_dir = 'schemas/'
#     repos = [['mod-inventory-storage', 'inv'],
#              ['data-import-raml-storage', 'srs'],
#              ['data-import-raml-storage', 'srm'],
#              ['raml', 'meta']]
#     [shutil.rmtree(dir, ignore_errors=True) for dir in [temp_dir, base_dir]]
#     for r,p = in [[r,f"{Path(temp_dir, r)}"] for r,p in repos]:
#         cmd = f"git clone https://github.com/folio-org/{r} {p}"
#         run(cmd.split(), check=True)
#         if p = 'inv':
#             dir = Path(temp_dir, p, 'mod-inventory-storage/ramls')
#             for f in dir.glob(f"*.json"):
#                 Path(base_dir, p).mkdir(parents=True, exist_ok=True)
#                 shutil.copy(f, Path(base_dir, p))
#                 if f.name in ['instance.json', 'holdingsrecord.json',
#                               'item.json']:
#                     shutil.copy(f, Path(base_dir))
#         if p = 'meta':
#             dir = Path(temp_dir, p, 'raml/schemas')
#             for f in dir.glob(f"*.schema"):
#                 Path(base_dir, p).mkdir(parents=True, exist_ok=True)
#                 shutil.copy(f, Path(base_dir, p))
#                 if f.name in ['metadata.json']:
#                     d = Path(base_dir,'schemas/raml-util/schemas')
#                     d.mkdir(parents=True, exist_ok=True)
#                     shutil.copy(f,d)
#         if p = 'srs':
#             dir = Path(temp_dir, p, 'schemas/mod-source-record-storage')
#             for f in dir.glob(f"*.json"):
#                 Path(base_dir, p).mkdir(parents=True, exist_ok=True)
#                 shutil.copy(f, Path(base_dir, p))
#                 if f.name in ['parsedRecord.json', 'rawRecord.json'
#                               'snapshot.json', 'recordType.json',
#                               'errorRecord.json', 'recordMedel.json']:
#                     d = Path(base_dir,'schemas/raml-util/schemas')
#                     d.mkdir(parents=True, exist_ok=True)
#                     shutil.copy(f,d)
#             dir = Path(temp_dir, p, 'schemas/common')
#             for f in dir.glob(f"*.json"):
#                 if f.name in ['uuid.json']:
#                     d = Path(base_dir, p, 'schemas/common')
#                     d.mkdir(parents=True, exist_ok=True)
#                     shutil.copy(f,d)
#         if p = 'srm':
#             dir = Path(temp_dir, p, 'schemas/mod-source-record-manager')
#             for f in dir.glob(f"*.json"):
#                 Path(base_dir, p).mkdir(parents=True, exist_ok=True)
#                 shutil.copy(f, Path(base_dir, p))
#                 if f.name in ['jobExecution.json',
#                               'jobExecutionSourceChunk.json']:
#                     d = Path(base_dir,'schemas/raml-util/schemas')
#                     d.mkdir(parents=True, exist_ok=True)
#                     shutil.copy(f,d)

#             for f in Path(temp_dir, d['ref_dir'], d['repo_dir']).glob(f"*.{d['suffix']}"):
#                 shutil.copy(f, Path(base_dir, d['ref_dir']))
#             Path(base_dir, d['work_dir']).mkdir(parents=True, exist_ok=True)
#             for f in [Path(temp_dir, d['ref_dir'], d['repo_dir'], f) for f in d['work_files']]:
#                 shutil.copy(f, Path(base_dir, d['work_dir']))

#     l = [{'ref_dir': 'inv',
#           [{'filter': 'schemas/mod-source-record-storage',
#             'files': ['*']
#             'dest': 'inv'},
#           {'filter': 'schemas/mod-source-record-storage',
#             'files': ['instance.json', 'holdingsrecord.json', 'item.json']
#             'dest': ''}]

#     l = [{'repo': 'mod-inventory-storage',
#           'repo_dir': 'ramls',
#           'suffix': 'json',
#           'ref_dir': 'inv',
#           'work_files':['instance.json','holdingsrecord.json','item.json']},

#          {'repo': 'raml',
#           'repo_dir': 'schemas',
#           'suffix': 'schema',
#           'ref_dir': 'meta',
#           'work_files': ['raml-util/schemas/tags.schema',
#                          'raml-util/schemas/metadata.schema']},

#          # {'repo': 'data-import-raml-storage',
#          #  'repo_dir': 'schemas/mod-source-record-storage',
#          #  'suffix': 'json',
#          #  'ref_dir': 'srs',
#          #  'work_files': []},

#          {'repo': 'data-import-raml-storage',
#           'clone_dir': 'srs',
#           'copy_paths': [{'from': 'schemas/mod-source-record-storage/*', 'to': 'srs'}
#                          {'from': 'schemas/mod-source-record-storage/*', 'to': 'srs'}
                         
#           'repo_dir': 'schemas/mod-source-record-storage',
#           'suffix': 'json',
#           'ref_dir': 'srs',
#           'work_files': []},

#          # {'repo': 'data-import-raml-storage',
#          #  'repo_dir': 'schemas/mod-source-record-manager',
#          #  'suffix': 'json',
#          #  'ref_dir': 'srm',
#          #  'work_files': ['parsedRecord.json', 'common/uuid.json']}]
          
#          {'repo': 'data-import-raml-storage',
#           'clone_dir': 'srm',
#           'file_paths': 
#           'repo_dir': 'schemas/mod-source-record-manager',
#           'ref_dir': 'srm',
#           'suffix': 'json',
#           'work_files': ['parsedRecord.json', 'common/uuid.json']}]
          
#     if m.fetch_latest_json_schemas:
#         [shutil.rmtree(dir, ignore_errors=True) for dir in [temp_dir, base_dir]]
#         for d in l:
#             cmd = f"git clone https://github.com/folio-org/{d['repo']} " \
#                   f"{Path(temp_dir, d['ref_dir'])}"
#             run(cmd.split(), check=True)
#             Path(base_dir, d['ref_dir']).mkdir(parents=True, exist_ok=True)
#             for f in Path(temp_dir, d['ref_dir'], d['repo_dir']).glob(f"*.{d['suffix']}"):
#                 shutil.copy(f, Path(base_dir, d['ref_dir']))
#             Path(base_dir, d['work_dir']).mkdir(parents=True, exist_ok=True)
#             for f in [Path(temp_dir, d['ref_dir'], d['repo_dir'], f) for f in d['work_files']]:
#                 shutil.copy(f, Path(base_dir, d['work_dir']))


@timing
def load_json_schemas(m):
    m.schema = dict()
    schema_path = m.cur_path/'schemas'
    for p in schema_path.iterdir():
        if p.is_file():
            with open(p) as f:
                j = jsonref.loads(f.read(), base_uri=p.as_uri(), jsonschema=True)
                m.schema[p.stem] = j
    m.validate_instance = fastjsonschema.compile(m.schema['instance'])
    # m.validate_marc = fastjsonschema.compile(m.schema['parsedRecord'])
    m.validate_holdings = fastjsonschema.compile(m.schema['holdingsrecord'])
    m.validate_item = fastjsonschema.compile(m.schema['item'])


                
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


def loc_to_utc(date):
    if isinstance(date, datetime.datetime):
        if not date.tzinfo:
            date = pendulum.instance(date).set(tz='local')
            date = date.in_tz('utc')
        else:
            date = pendulum.instance(date)
            date = date.in_tz('utc')
    elif not date:
        date = pendulum.now('utc')
    else:
        raise Exception ("%s is not of type datetime" % date) 
    return date.replace(microsecond=0)


def date_to_str(date):
    if isinstance(date, datetime.datetime):
        if not date.tzinfo:
            date = pendulum.instance(date).set(tz='local')
            date = date.in_tz('utc')
        else:
            date = pendulum.instance(date)
            date = date.in_tz('utc')
    elif not date:
        date = pendulum.now('utc')
    else:
        raise Exception ("%s is not of type datetime" % date) 
    return date.replace(microsecond=0).to_iso8601_string()


def serialize_datatype(obj):
    if isinstance(obj, datetime.datetime):
        return date_to_str(obj)
    else:
        type_name = obj.__class__.__name__
        raise TypeError("Object of type '%s' is not JSON serializable" % type_name)


# def serialize_datatype(obj):
#     if isinstance(obj, datetime.datetime):
#         return obj.strftime("%Y-%m-%dT%H:%M:%S")
#     else:
#         type_name = obj.__class__.__name__
#         raise TypeError("Object of type '%s' is not JSON serializable" % type_name)


def create_batches(m, rows):
    return [rows[i:i+m.batch_size] for i in range(0,len(rows),m.batch_size)]


def truncate_rows(m):
    # lst = ['item', 'holdings_record', 'instance',
    #        'job_execution_source_chunks', 'job_executions',
    #        'error_records', 'marc_records', 'raw_records', 'records',
    #        'snapshots']
    lst = ['item', 'holdings_record', 'instance']
    for table in lst:
        m.pdb.cur.execute('truncate table %s cascade;' % table)
        m.pdb.con.commit()
    

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
    def __init__(self, m):
        self.dsn = "host='%s' dbname='folio' user='folio'" % m.folio_host
        self.con = psycopg2.connect(self.dsn)
        self.cur = self.con.cursor()


def fetch_index_defs(m):
    #
    cols = ['id','schema_name','table_name','index_name','index_def']
    sql = "select * from index_def where index_name not in " \
          "('instance_pkey', 'holdings_record_pkey', 'item_pkey') " \
          "order by id;"
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()
    m.indexes = [dict(zip(cols,r)) for r in m.pdb.cur.fetchall()]
    m.index_dict = dict([[t,[d for d in m.indexes if d['table_name'] == t]] for t in m.index_tables])
    

@timing 
def initialize_pre_load(m):
    for d in m.indexes:
        m.pdb.cur.execute("drop index if exists %s;" % d['index_name'])
        m.pdb.con.commit()
    m.pdb.cur.execute("set session_replication_role to replica;")
    for t in m.index_tables: m.pdb.cur.execute("alter table %s disable trigger all" % t)
    # test unlogging tables for performance
    # for t in m.index_tables: m.pdb.cur.execute("alter table %s set unlogged" % t)
    m.pdb.con.commit()
         

@timing 
def initialize_post_load(m):  # debug
    for d in m.indexes:
        if d['index_name'] not in ['instance_contributors_idx']:
            m.pdb.cur.execute(d['index_def'])
            m.pdb.con.commit()
    m.pdb.cur.execute("set session_replication_role to default;")
    for t in m.index_tables: m.pdb.cur.execute("alter table %s enable trigger all" % t)
    # test unlogging tables for performance
    # for t in m.index_tables: m.pdb.cur.execute("alter table %s set logged" % t)
    m.pdb.con.commit()
    m.pdb.cur.execute('analyze')
    m.pdb.con.commit()
    

def initialize_normal_load(m):
    schemas = ['diku_mod_calendar', 'diku_mod_circulation_storage',
               'diku_mod_configuration', 'diku_mod_inventory_storage',
               'diku_mod_login', 'diku_mod_notes', 'diku_mod_notify',
               'diku_mod_permissions', 'diku_mod_users', 'diku_mod_vendors', 'public']
    m.pdb.cur.execute("set search_path to %s;" % ','.join(schemas))
    m.pdb.cur.execute("alter table instance disable trigger set_instance_hrid;")
    m.pdb.cur.execute("alter table holdings_record disable trigger set_holdings_record_hrid;")
    m.pdb.cur.execute("alter table item disable trigger set_item_hrid;")


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


##### uuids        


@timing 
def map_uuids(m):
    btime = time()
    map_uuids_to_tables(m)
    if m.update_uuids:
        create_uuid_table(m)
        map_uuids_to_identifiers(m)
        delete_old_uuids(m)
        add_new_uuids(m)
        create_uuid_indexes(m)
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


def map_uuids_to_tables(m):
    # Specify count number of uuids for Folio reference tables.
    l1 = [{'tbl':'alternative_title_type','count':3},
          {'tbl':'identifier_type','count':17},
          {'tbl':'contributor_name_type','count':3},
          {'tbl':'contributor_type','count':268},
          {'tbl':'classification_type','count':4},
          {'tbl':'instance_type','count':25},
          {'tbl':'instance_format','count':56},
          {'tbl':'mode_of_issuance','count':7},
          {'tbl':'instance_status','count':19},
          {'tbl':'call_number_type','count':12},
          {'tbl':'holdings_type','count':2},
          {'tbl':'material_type','count':8},
          {'tbl':'electronic_access_relationship','count':5},
          {'tbl':'statistical_code_type','count':2},
          {'tbl':'statistical_code','count':27},
          # {'tbl':'instance_source_marc','count':1},
          # {'tbl':'ext_ownership_type','count':3},
          {'tbl':'locinstitution','count':1},
          {'tbl':'loccampus','count':1},
          {'tbl':'loclibrary','count':15},
          {'tbl':'location','count':227},
          {'tbl':'loan_type','count':72},
          {'tbl':'ill_policy','count':1},
          {'tbl':'instance_note_type','count':53},
          {'tbl':'holdings_note_type','count':7},
          {'tbl':'item_note_type','count':7}]
    for d in l1:
        d['col'] = 'id'
        d['type'] = 'ref'

    # Specify uuids for each of the ids retrieved from table selects
    l2 = [{'tbl':'ole_ds_bib_t', 'col':'bib_id',
           'sql': 'select bib_id from ole_ds_bib_t'},
          {'tbl':'ole_ds_holdings_t', 'col':'holdings_id',
           'sql':'select holdings_id from ole_ds_holdings_t'},
          {'tbl':'ole_ds_item_t', 'col':'item_id',
           'sql':'select item_id from ole_ds_item_t'},
          {'tbl':'ole_circ_dsk_dtl_t', 'col':'crcl_dsk_dtl_id',
           'sql':'select crcl_dsk_dtl_id from ole_circ_dsk_dtl_t'},
          {'tbl':'ole_crcl_dsk_locn_t', 'col':'ole_crcl_dsk_locn_id',
           'sql':'select ole_crcl_dsk_locn_id from ole_crcl_dsk_locn_t'},
          {'tbl':'ole_crcl_dsk_t', 'col':'ole_crcl_dsk_id',
           'sql':'select ole_crcl_dsk_id from ole_crcl_dsk_t'},
          {'tbl':'ole_cat_bib_record_stat_t', 'col':'bib_record_stat_id',
           'sql':'select bib_record_stat_id from ole_cat_bib_record_stat_t'},
          {'tbl':'order_item', 'col':'id',
           'sql':"select c.uc_item_id as id " \
           "from pur_po_itm_t oi " \
           "join pur_po_t o on o.fdoc_nbr = oi.fdoc_nbr " \
           "join krew_doc_hdr_t d on d.doc_hdr_id = o.fdoc_nbr " \
           "join ole_copy_t c on c.po_itm_id = oi.po_itm_id " \
           "where oi.itm_typ_cd = 'ITEM' " \
           "and oi.itm_actv_ind = 'Y' " \
           "and o.po_cur_ind = 'Y' " \
           # "and d.app_doc_stat = 'Open' " \
           "and c.uc_item_id is not null"}]
    for d in l2: d['type'] = 'bib'
    m.tables = l1 + l2


def map_uuids_to_identifiers(m):
    for d in m.tables:
        if d['type'] == 'ref':
            sql = "select id from cat.uuid where table_name = '%s'" % d['tbl']
            m.mdb.cur.execute(sql)
            old_ids = set([i for r in m.mdb.cur.fetchall() for i in r])
            new_ids = set(list(range(1, d['count'] + 1)))
            d['del'] = sorted(list(old_ids - new_ids))
            d['add'] = sorted(list(new_ids - old_ids))
            d['type'] = 'ref'
        elif d['type'] == 'bib':
            m.mdb.cur.execute(d['sql'])
            ole_ids = set([int(i) for r in m.mdb.cur.fetchall() for i in r])
            sql = "select id from cat.uuid where table_name = '%s'" % d['tbl']
            m.mdb.cur.execute(sql)
            cat_ids = set([i for r in m.mdb.cur.fetchall() for i in r])
            d['add'] = sorted(list(ole_ids - cat_ids))
            d['del'] = sorted(list(cat_ids - ole_ids))

    
def delete_old_uuids(m):
    for d in m.tables:
        btime = time()
        for b in create_batches(m, d['del']):
            s = ','.join(['%s'] * len(b))
            sql = "delete from cat.uuid where table_name = '%s' and id in (%s)" % (d['tbl'], s)
            m.mdb.cur.execute(sql,b)
            m.mdb.cur.execute('commit')
        print("Number of uuids deleted for %s: %d" % (d['tbl'],len(d['del'])))
        calc_time(m, "Time taken to delete rows", btime)
            
    
def add_new_uuids(m):
    sql = "insert into cat.uuid (id, uuid, table_name, column_name) values (%s, %s, %s, %s);"
    for d in m.tables:
        btime = time()
        for b in create_batches(m, d['add']):
            rows = [[id, str(uuid4()), d['tbl'], d['col']] for id in b]
            m.mdb.cur.executemany(sql,rows)
            m.mdb.cur.execute('commit')
        print("Number of uuids added for %s: %d" % (d['tbl'],len(d['add'])))
        calc_time(m, "Time taken to add uuids", btime)
    

def create_uuid_indexes(m):
    if m.create_uuid_table:
        btime = time()
        for col in ['id', 'uuid', 'table_name', 'column_name']:
            unique = 'unique' if col == 'uuid' else ''
            sql = "create %s index %s using btree on cat.uuid(%s);" % (unique, col,col)
            m.mdb.cur.execute(sql)
        calc_time(m, "Time taken to create uuid indexes", btime)


@timing
def map_marc_rows(m):
    for b in m.bl:
        m.b = b
        r = b['o']['r']
        try:
            map_xml_to_json(m)
            create_marc_row(m)
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
            if b['include']:
                e = 'unable to map bib row: %d' % r['bib_id']
                m.log.error(e, exc_info=m.stack_trace)
                m.log.info(pformat(r))
                b['include'] = False

        
def map_xml_to_json(m):
    j = dict([['id', m.b['o']['r']['uuid']], ['content', dict()]])
    for a in m.b['o']['d']:
        if a.tag in ['leader']:
            j['content']['leader'] = str(a.text)
            j['content']['fields'] = []
        elif a.tag in ['controlfield']:
            tag, txt = (a.attrib['tag'], str(a.text))
            d = dict([[tag, txt]])
            j['content']['fields'].append(d)
        elif a.tag in ['datafield']:
            tag, ind1, ind2 = (a.attrib['tag'],
                               a.attrib['ind1'], a.attrib['ind2'])
            d = dict([[tag, dict([['ind1', ind1],
                                  ['ind2', ind2], ['subfields', []]])]])
            for b in a:
                if b.tag in ['subfield']:
                    sf, txt = (b.attrib['code'], str(b.text))
                    subd = dict([[sf, txt]])
                    d[tag]['subfields'].append(subd)
            j['content']['fields'].append(d)
    sfs = [dict([['i', m.b['o']['r']['uuid']]]),
           dict([['s', m.b['o']['r']['uuid']]])]
    d = dict([['999', dict([['ind1', 'f'], ['ind2', 'f'],
                            ['subfields', sfs]])]])
    j['content']['fields'].append(d)
    m.b['j']['d'] = j


#@timing
def create_marc_row(m):
    if 'createdByUserId' in m.b['f']['d']['metadata']:
        created_by = m.b['f']['d']['metadata']['createdByUserId']
    else: created_by = None
    d = dict([['_id', m.b['o']['r']['uuid']],
              ['jsonb', m.b['j']['d']],
              ['creation_date', loc_to_utc(m.b['o']['r']['date_created'])],
              ['created_by', created_by],
              ['bib_id', m.b['o']['r']['bib_id']]])
    m.b['j']['r'] = d


@timing
def format_marc_rows(m):
    for b in [b for b in m.bl if b['include']]:
        r = b['j']['r']
        try:
            r['jsonb'] = json.dumps(r['jsonb'], default=serialize_datatype)
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
            m.log.error("error in marc row %s" % r['bib_id'], exc_info=m.stack_trace)
            b['include'] = False

            
@timing
def validate_marc_rows(m):
    if not m.validate: return
    for b in [b for b in m.bl if b['include']]:
        r = b['j']['r']
        try:
            m.validate_marc(r['jsonb'])
        except:
            m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
            m.log.error("error in marc row %s" % r['bib_id'],
                        exc_info=m.stack_trace)
            b['include'] = False

            
@timing
def save_marc_rows(m):
    l = [b for b in m.bl if b['include']]
    if not l: return
    count = len(l)
    cols = m.col['marc_records']
    sql = "insert into marc_records values %s"
    rows = [list(b['j']['r'].values())[:-1] for b in l]
    template = '(%s)' % ', '.join(['%s'] * len(cols))
    try:
        execute_values(m.pdb.cur, sql, rows,
                       template=template, page_size=count)
        m.pdb.con.commit()
        m.log.debug("saved marc records")
    except:
        m.pdb.con.rollback()
        sql = "insert into marc_records values (%s)" % \
              ','.join(['%s'] * len(cols))
        for b in l:
            r = b['j']['r']
            try:
                m.pdb.cur.execute(sql, list(r.values())[:-1])
                m.pdb.con.commit()
            except:
                m.pdb.con.rollback()
                r['jsonb'] = json.loads(r['jsonb'])
                m.reject_file.write('%s\t%s\t%s\n' % (r['bib_id'], None, None))
                e = "error in instance row %s" % r['bib_id']
                m.log.error(e, exc_info=m.stack_trace)
                m.log.debug(pformat(r))
                b['include'] = False
        

def map_json_to_binary(m):
    j = m.b['j']['d']
    st, ft, rt = ('\x1f', '\x1e', '\x1d')
    d = dict([['ldr', []], ['dir', []], ['ctr', []], ['dat', []]])
    d['ldr'] = j['content']['leader']
    pos = 0
    for t,f in [[a for b in d.items() for a in b] for d in j['content']['fields']]:
        if t < '010':
            s = f"{f}{ft}"
            d['ctr'].append(s)
        else:
            lst = [[a for b in d.items() for a in b] for d in f['subfields']]
            sfs = ''.join([f'{st}{k}{v}' for k,v in lst])
            s = f"{f['ind1']}{f['ind2']}{sfs}{ft}"
            d['dat'].append(s)
        slen = len(s)
        entry = f"{t}{slen:04}{pos:05}"
        d['dir'].append(entry)
        pos += slen
    r = f"{d['ldr']}{''.join(d['dir'])}{ft}{''.join(d['ctr'])}{''.join(d['dat'])}{rt}"
    m.b['r']['d'] = r.encode()

        
def map_binary_to_json(m):
    r = m.b['r']['d'].decode('utf-8')
    st, ft, rt = ('\x1f', '\x1e', '\x1d')
    l = [s for s in re.split(ft, r[:-1]) if s]
    ldr, dir, flds = (l[0][0:24], l[0][24:], l[1:])
    tags = [dir[i:i+3] for i in range(0, len(flds)*12, 12)]
    j = dict([['leader', ldr], ['fields', []]])
    for k,v in zip(tags, flds):
        if k < '010':
            j['fields'].append(dict([[k, v]]))
        else:
            l = re.split(st, v)
            ind1, ind2 = list(l[0])
            d = dict([['ind1', ind1], ['ind2', ind2]])
            d['subfields'] = [dict([[s[0], s[1:]] for s in l[1:]])]
            j['fields'].append(dict([[k, d]]))
    return json.dumps(j, indent=4)


def error_records(m):
    pass


def raw_records(m):
    # binary_marc
    j = {'id': None,
         'content': None}
    

def records(m):
    j = {'id': m.b['o']['r']['srs_uuid'],
         'deleted': False,
         'matchedId': m.b['o']['r']['srs_uuid'],
         'generation': 0,
         'recordType': 'MARC',
         'snapshotId': m.b['o']['r']['srs_uuid'],
         'rawRecordId': m.b['o']['r']['srs_uuid'],
         'parsedRecordId': m.b['o']['r']['srs_uuid'],
         'additionalInfo': {'suppressDiscovery': False},
         'parseRecordId': '1b74ab75-9f41-4837-8662-a1d99118008d'}


def snapshots(m):
    j = {
        "status": "COMMITTED",
        "jobExecutionId": "00000000-0000-0000-0000-000000000000",
        "processingStartedDate": "2019-01-01T12:00:00.000"
    } 
        
        
def job_execution_source_chunks(m):
    j = {
        "id": "3e3c0b0f-3a2b-4547-ac96-4e7a4633b90a",
        "last": false,
        "state": "COMPLETED",
        "chunkSize": 1,
        "createdDate": "2019-10-19T05:15:53.607+0000",
        "completedDate": "2019-10-19T05:15:54.647+0000",
        "jobExecutionId": "2c799153-5f23-4dc1-95ed-d31a25d7307a",
        "processedAmount": 1
    }


def job_executions(m):
    j ={
        "id": "e451348f-2273-4b45-b385-0a8b0184b4e1",
        "hrId": "112984432",
        "runBy": {
            "lastName": "Doe",
            "firstName": "John"
        },
        "status": "PROCESSING_FINISHED",
        "progress": {
            "total": 20000,
            "current": 20000
        },
        "uiStatus": "READY_FOR_PREVIEW",

        "startedDate": "2018-11-20T23:26:44.000",
        "completedDate": "2018-11-21T00:38:44.000",
        "jobProfileInfo": {
            "id": "448ae575-daec-49c1-8041-d64c8ed8e5b1",
            "name": "Authority updates",
            "dataType": "MARC"
        },
        "subordinationType": "PARENT_SINGLE"
    }


def journal_records(m):
    pass


def mapping_rules(m):
    pass


# if __name__ == '__main__':
#     m = main()

