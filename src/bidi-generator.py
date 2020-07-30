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
#################################################################### processing

async def process ():
    # instances
    prefix = 'n:'
    dh   = await model_ensure_instance(prefix, 'gfb:District_Heat' , escape('district_heat'))
    hx   = await model_ensure_instance(prefix, 'gfb:Heat_Exchanger', escape('heat_exchanger'))
    tank      = await model_ensure_instance(prefix, 'gfb:Hot_Water_Tank'          , escape('hot_water_tank'))
    tank_temp = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor', escape('hot_water_tank_temperature'))
    d_ret_flow  = await model_ensure_instance(prefix, 'gfb:Water_Flow_Sensor'                 , escape('district_flow'))
    d_ret_temp  = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor'          , escape('district_return_temp'))
    d_sup_temp  = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor'          , escape('district_supply_temp'))
    d_ret_valve = await model_ensure_instance(prefix, 'gfb:Valve'                             , escape('district_return_valve'))
    d_ret_pres  = await model_ensure_instance(prefix, 'gfb:Water_Pressure_Sensor'             , escape('district_return_pressure'))
    d_sup_pres  = await model_ensure_instance(prefix, 'gfb:Water_Pressure_Sensor'             , escape('district_supply_pressure'))
    d_dif_pres  = await model_ensure_instance(prefix, 'gfb:Water_Differential_Pressure_Sensor', escape('district_differential_pressure'))
    d_heat      = await model_ensure_instance(prefix, 'gfb:Heat_Meter'                        , escape('district_heat_meter'))
    h_sup_temp = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor', escape('heated_supply_temp'))
    h_l1_sup_valve = await model_ensure_instance(prefix, 'gfb:Valve'                   , escape('heated_loop1_supply_valve'))
    h_l1_sup_temp  = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor', escape('heated_loop1_supply_temp'))
    h_l1_sup_pump  = await model_ensure_instance(prefix, 'gfb:Pump'                    , escape('heated_loop1_supply_pump'))
    h_l1_radiator  = await model_ensure_instance(prefix, 'gfb:Radiator'                , escape('heated_loop1_radiator'))
    h_l2_sup_bvalve    = await model_ensure_instance(prefix, 'gfb:Bypass_Valve'            , escape('heated_loop2_supply_bypass_valve'))
    h_l2_sup_temp      = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor', escape('heated_loop2_supply_temp'))
    h_l2_ret_pump      = await model_ensure_instance(prefix, 'gfb:Pump'                    , escape('heated_loop2_return_pump'))
    h_l2_ret_temp_pre  = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor', escape('heated_loop2_return_prebypass_temp'))
    h_l2_ret_temp_post = await model_ensure_instance(prefix, 'gfb:Water_Temperature_Sensor', escape('heated_loop2_return_postbypass_temp'))
    h_l2_radiator      = await model_ensure_instance(prefix, 'gfb:Radiator'                , escape('heated_loop2_radiator'))
    h_l3_ret_valve = await model_ensure_instance(prefix, 'gfb:Valve', escape('heated_loop3_return_valve'))
    
    # relations: district heated water
    await model_ensure_relationship(dh         , 'gfb:feedsSupplyDistrictHeatedWater', d_sup_temp)
    await model_ensure_relationship(d_sup_temp , 'gfb:feedsSupplyDistrictHeatedWater', d_sup_pres)
    await model_ensure_relationship(d_sup_pres , 'gfb:feedsSupplyDistrictHeatedWater', hx)
    await model_ensure_relationship(hx         , 'gfb:feedsReturnDistrictHeatedWater', d_ret_pres)
    await model_ensure_relationship(d_ret_pres , 'gfb:feedsReturnDistrictHeatedWater', d_ret_valve)
    await model_ensure_relationship(d_ret_valve, 'gfb:feedsReturnDistrictHeatedWater', d_ret_temp)
    await model_ensure_relationship(d_ret_temp , 'gfb:feedsReturnDistrictHeatedWater', d_ret_flow)
    await model_ensure_relationship(d_sup_temp, 'gfb:controls', d_heat)
    await model_ensure_relationship(d_ret_temp, 'gfb:controls', d_heat)
    await model_ensure_relationship(d_ret_flow, 'gfb:controls', d_heat)
    await model_ensure_relationship(d_sup_pres, 'gfb:controls', d_dif_pres)
    await model_ensure_relationship(d_ret_pres, 'gfb:controls', d_dif_pres)
    
    # relations: heated
    await model_ensure_relationship(hx, 'gfb:feedsSupplyHeatedWater', h_sup_temp)
    
    # relations: heated loop 1
    await model_ensure_relationship(h_sup_temp    , 'gfb:feedsSupplyHeatedWater', h_l1_sup_valve)
    await model_ensure_relationship(h_l1_sup_valve, 'gfb:feedsSupplyHeatedWater', h_l1_sup_temp)
    await model_ensure_relationship(h_l1_sup_temp , 'gfb:feedsSupplyHeatedWater', h_l1_sup_pump)
    await model_ensure_relationship(h_l1_sup_pump , 'gfb:feedsSupplyHeatedWater', h_l1_radiator)
    await model_ensure_relationship(h_l1_radiator , 'gfb:feedsReturnHeatedWater', hx)
    
    # relations: heated loop 2
    await model_ensure_relationship(h_sup_temp        , 'gfb:feedsSupplyHeatedWater', h_l2_sup_bvalve)
    await model_ensure_relationship(h_l2_sup_bvalve   , 'gfb:feedsSupplyHeatedWater', h_l2_sup_temp)
    await model_ensure_relationship(h_l2_sup_temp     , 'gfb:feedsSupplyHeatedWater', h_l2_radiator)
    await model_ensure_relationship(h_l2_radiator     , 'gfb:feedsReturnHeatedWater', h_l2_ret_pump)
    await model_ensure_relationship(h_l2_ret_pump     , 'gfb:feedsReturnHeatedWater', h_l2_ret_temp_pre)
    await model_ensure_relationship(h_l2_ret_temp_pre , 'gfb:feedsReturnHeatedWater', h_l2_sup_bvalve)
    await model_ensure_relationship(h_l2_ret_temp_pre , 'gfb:feedsReturnHeatedWater', h_l2_ret_temp_post)
    await model_ensure_relationship(h_l2_ret_temp_post, 'gfb:feedsReturnHeatedWater', hx)
    
    # relations: heated loop 3
    await model_ensure_relationship(h_sup_temp    , 'gfb:feedsSupplyHeatedWater', tank)
    await model_ensure_relationship(tank_temp     , 'brick:isPointOf'           , tank)
    await model_ensure_relationship(tank          , 'gfb:feedsReturnHeatedWater', h_l3_ret_valve)
    await model_ensure_relationship(h_l3_ret_valve, 'gfb:feedsReturnHeatedWater', hx)
    
    # store resulting model
    await rdf_store()

def escape (value: str):
    value = value.replace(' ', '_space_')
    value = value.replace(':', '_colon_')
    value = value.replace('/', '_slash_')
    value = value.replace('æ', '_ae_')
    value = value.replace('ø', '_ao_')
    value = value.replace('å', '_aa_')
    value = value.replace('Æ', '_AE_')
    value = value.replace('Ø', '_AO_')
    value = value.replace('Å', '_AA_')
    return value

###############################################################################
######################################################################### model

async def model_ensure_instance (prefix: str, datatype: str, label: str):
    q = '''
    SELECT ?name
    WHERE {
        ?name rdf:type %s .
        ?name rdf:label "%s" .
    }
    ''' % (datatype, label)
    success, rs = await rdf_query(q)
    
    if success and len(rs)>0:
        name = rs[0][0]
        if '#' in name:
            parts = name.split('#')
            if len(parts)!=2:
                print('ERROR: Unknown format of entity "%s".' % name)
            parts[0] += '#'
            if parts[0] in namespaces:
                name = '%s:%s' % (namespaces[parts[0]], parts[1])
            else:
                print('ERROR: Namespace not defined for entity "%s".' % name)
    else:
        name = '%s_%s' % (prefix, label)
        await rdf_update_split(insert_clause=['%s rdf:type %s' % (name, datatype),
                                              '%s rdf:label "%s"' % (name, label)])
    return name

async def model_ensure_relationship (sub: str, pred: str, obj: str, onlyobj=False):
    if onlyobj:
        await rdf_update_split(delete_clause=['%s %s ?obj' % (sub, pred)],
                               where_clause=['%s %s ?obj' % (sub, pred)])
    
    await rdf_update_split(insert_clause=['%s %s %s' % (sub, pred, obj)])

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
    
    await process()

# guard: python version
if not valid_python_version():
    print('ERROR: Invalid python version (%s), bust be 3.(5+).' % str(sys.version_info))
    sys.exit(1)

# guard: commandline arguments
if len(sys.argv) != 4:
    print('Syntax: %s NAMESPACE RDF_SERVER_HOST RDF_SERVER_PORT' % sys.argv[0])
    print('        %s http://ss.sdu.dk/test/grundstrup-bidi/20200727/# 127.0.0.1 8001' % sys.argv[0])
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
