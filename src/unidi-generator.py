#!/usr/bin/env python3

import sys
import asyncio
import aiohttp
import json

namespaces = {}

###############################################################################
####################################################################### helpers

def valid_python_version ():
    v = sys.version_info
    if v[0] != 3: return False
    if v[1] < 5: return False
    return True

###############################################################################
########################################################################### rdf

async def rdf_namespaces ():
    url = 'http://%s:%u/namespaces' % (host, port)
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data='"dummy"') as response:
            text = await response.text()
            
            if response.status != 200:
                print('ERROR: Unable to lookup namespaces')
                print(text)
                return False, {}
            
            try:
                return True, json.loads(text)
            except Exception as e:
                print('ERROR: Exception while trying to parse result of namespace lookup')
                print(text)
                print(str(e))
                return False, {}

async def rdf_store ():
    url = 'http://%s:%u/store' % (host, port)
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data='"dummy"') as response:
            text = await response.text()
            
            if response.status != 200:
                print('ERROR: Unable to store model')
                print(text)
                return False
            
            print('NOTICE: Successfully stored model')
            return True

async def rdf_query (query: str):
    query = json.dumps(query, sort_keys=True, indent=4, separators=(',', ': '))
    url = 'http://%s:%u/query' % (host, port)
    async with aiohttp.ClientSession() as session:
        async with session.put(url, data=query) as response:
            text = await response.text()
            
            if response.status != 200:
                print('ERROR: Unable to query model')
                print(query)
                print(text)
                1/0
                return False, None
            
            try:
                return True, json.loads(text)['resultset']
            except Exception as e:
                print('ERROR: Exception while trying to parse result model query')
                print(text)
                print(str(e))
                return False, None

async def rdf_update (query: str):
    query = json.dumps(query, sort_keys=True, indent=4, separators=(',', ': '))
    url = 'http://%s:%u/update' % (host, port)
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=query) as response:
            text = await response.text()
            
            if response.status != 200:
                print('ERROR: Unable to update model with status "%d"' % response.status)
                print(text)
                1/0
                return False
            
            try:
                r = json.loads(text)
                if not 'success' in r or type(r['success'])!=bool:
                    print('ERROR: Unable to parse result of model update')
                    print(text)
                    return False
                
                print('NOTICE: Success of model update: '+str(r['success']))
                return r['success']
            except:
                print('ERROR: Exception while trying to parse result of model update')
                print(text)
                print(str(e))
                return False

async def rdf_update_split (ns : list = None, insert_clause : str = None, delete_clause : str = None, where_clause : str = None):
    if ns == None: ns = []
    
    q = []
    
    for entry in ns:
        q.append('%s' % entry)
    q.append('')
    
    if insert_clause:
        q.append('INSERT {')
        for entry in insert_clause:
            q.append('    %s .' % entry)
        q.append('}')
    
    if delete_clause:
        q.append('DELETE {')
        for entry in delete_clause:
            q.append('    %s .' % entry)
        q.append('}')
    
    q.append('WHERE {')
    if where_clause:
        for entry in where_clause:
            q.append('    %s .' % entry)
    q.append('}')
    
    q = ''.join(map(lambda line: '%s\n' % line, q))
    print(q)
    
    r = await rdf_update(q)
    print('response:', r)

###############################################################################
########################################################################## main

async def main():
    global namespaces
    
    # load namespaces
    print('STATUS: Loading namespaces:')
    success, ns = await rdf_namespaces()
    if not success or not 'success' in ns or not ns['success']:
        print('ERROR: Unable to fetch namespaces:')
        print(success)
        print(ns)
        sys.exit(3)
    for key in ns['namespaces']:
        prefix = ns['namespaces'][key]
        print('STATUS: - %s : %s' % (prefix, key))
        namespaces[prefix] = key
    
    print('done')

# guard: python version
if not valid_python_version():
    print('ERROR: Invalid python version (%s), bust be 3.(5+).' % str(sys.version_info))
    sys.exit(1)

# guard: commandline arguments
if len(sys.argv) != 4:
    print('Syntax: %s NAMESPACE RDF_SERVER_HOST RDF_SERVER_PORT' % sys.argv[0])
    print('        %s http://ss.sdu.dk/test/grundstrup-unidi/20200727/# 127.0.0.1 8001' % sys.argv[0])
    sys.exit(2)

# extract parameters
namespace   =     sys.argv[1]
host        =     sys.argv[2]
port        = int(sys.argv[3])

loop = asyncio.get_event_loop()

# enter service loop
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    print('')
    print('STATUS: Exiting ...')
    loop.close()
    exit(0)

########################################################################### EOF
###############################################################################
