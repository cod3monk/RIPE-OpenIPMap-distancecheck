#!/usr/bin/env python
'''
This script checks OpenIPMap data for plausability and new suggestions based on traceroute data.

Author: Julian Hammer <julian.hammer@fau.de>
License: AGPLv3
'''

from __future__ import print_function
from __future__ import division

import sys
import argparse
import urllib2
import json
from pprint import pprint
import os.path
import re
import pickle
import math
import ssl

import numpy as np
import yaml
import ipaddr

# http://stackoverflow.com/a/28048260/2754040
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def remote_msm_file(msm_id):
    url = "https://atlas.ripe.net/api/v1/measurement-latest/{}/".format(msm_id)
    return urllib2.urlopen(url)


def load_traceroute_file(fp):
    for l in fp.readlines():
        yield json.loads(l)


# http://stackoverflow.com/q/3844948/
def checkEqualIvo(lst):
    return not lst or lst.count(lst[0]) == len(lst)

#http://www.johndcook.com/blog/python_longitude_latitude/
def distance_on_unit_sphere(lat1, long1, lat2, long2):
    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0
         
    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
         
    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
         
    # Compute spherical distance from spherical coordinates.
         
    # For two locations in spherical coordinates 
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) = 
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length
     
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + 
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )
 
    # Remember to multiply arc by the radius of the earth 
    # in your favorite set of units to get length.
    # here in mean radius in kilometers
    return arc*6371.000785


def main():
    #data = load_traceroute(remote_msm_file(5010))
    raw_data = load_traceroute_file(open("tr_5010_1393628400-1393714800.txt"))
    if not os.path.isfile('rtt_map.pickle'):
        rtt_map = {}
        # 1. combine all RTTs between src and hop
        counter = 0
        for d in raw_data:
            # Make sure that hops are in right order
            d['result'].sort(key=lambda p: p['hop'])
            prb_id = d['prb_id']
            base_path = [prb_id]
            for r in d['result']:
                if 'result' not in r:
                    continue
                
                for p in r['result']:
                    if 'rtt' in p and 'from' in p and type(p['rtt']) is float:
                        path = tuple(base_path+[p['from']])
                        rtts = rtt_map.get(path, [])
                        rtts.append(p['rtt'])
                        rtt_map[path] = rtts
                
                # if this path seems unstable (origin of return packages changed), stop following it
                if len(r['result']) >= 1 and not checkEqualIvo(
                        [p['from'] for p in r['result'] if 'from' in p]):
                    break
                else:
                    base_path += [p['from'] for p in r['result'] if 'from' in p][:1]
                
            counter += 1
            if counter % 2500 == 0:
                print('.', end='')
                sys.stdout.flush()
        print()
        
        # 2. calculate 5th percentile 
        for k in rtt_map:
            rtt_map[k] = np.percentile(rtt_map[k], 5)
        
        # 3. dump data
        pickle.dump(rtt_map, open('rtt_map.pickle', 'w'))
    else:
        # Load data if already present/cached
        rtt_map = pickle.load(open('rtt_map.pickle'))
    
    # rtt_map: keys are a tuple of the path, with the first element as source
    #          values are the 5th percentile for this path
    
    if not os.path.isfile('filtered_paths.pickle'):
        # 4. calculate rtt distances between source and first public hops
        rfc1918 = re.compile(r'(^127\.0\.0\.1)|(^10\.)|(^172\.1[6-9]\.)|'
            r'(^172\.2[0-9]\.)|(^172\.3[0-1]\.)|(^192\.168\.)')
        
        paths = rtt_map.keys()
        
        # remove everything where non-rfc1918 appears as last hop and all others are rfc1918
        # or only two hosts are present in path
        paths = filter(lambda k: not rfc1918.match(k[-1]) and (len(k) == 2 or all(map(rfc1918.match, k[1:-1]))), paths)
        
        # dump data
        pickle.dump(paths, open('filtered_paths.pickle', 'w'))
    else:
        # Load data if already present/cached
        paths = pickle.load(open('filtered_paths.pickle'))
    
    for p in paths:
        last_oim = json.load(urllib2.urlopen('http://marmot.ripe.net/openipmap/ipmeta.json?ip='+p[-1], context=ctx))
        probe = json.load(urllib2.urlopen('https://atlas.ripe.net/api/v1/probe/?id='+str(p[0])))['objects'][0]
        print(rtt_map[p], p)
        if 'lat' in last_oim:
            dist = distance_on_unit_sphere(last_oim['lat'], last_oim['lon'], probe['latitude'], probe['longitude'])
            if dist > 50 and rtt_map[p] < 100:
                print('WRONG', p[-1], 'is probably not',dist,'km away from', probe['latitude'], probe['longitude'])
            else:
                print('GOOD')
        else:
            print('NEW', p[-1], 'is probably near', probe['latitude'], probe['longitude'])
    
    print(len(rtt_map), len(paths))
    
if __name__ == '__main__':
    main()
