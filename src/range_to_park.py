import ee
import ConfigParser
import time
import sys
import traceback
import json
import csv
import urllib
import urllib2

def saveRow(dat):

    host = 'http://mol.cartodb.com/api/v2/sql'

    fields = '(%s)' % ','.join(dat.keys())
    
    valList = []

    for k in dat.keys():
        v = dat[k]
        if isinstance(v,basestring):
            v =  '\'%s\'' % (v)

        valList.append(v)
    
    values = '(%s)' % ','.join([str(i) for i in valList])

    sql = 'insert into ' + _table + ' ' + fields + ' values ' + values
    print sql
    apiKey = '6132d3d852907530a3b047336430fc1999eb0f24'
    url = host + '?' + 'q=' + urllib.quote(sql,'') + '&api_key=' + apiKey

    #logging.info('Cartodb url: ' + url)

    #urllib2.urlopen(url)
    #print url
    
def rangeIntersect(image):
    masked = image.mask(_range);
    s = masked.reduceRegion(
                    reducer=ee.Reducer.sum(), 
                    geometry=_range.geometry(),
                    scale=1000,
                    maxPixels=10000000000,
                    bestEffort=True
                )
    
    return ee.Feature(None).set('intersect',s.get('b1')) #s is an object with a property called 'b1' that containst the value we want
##### end refine function

####################
### Main Program ###
####################
_inputfile = sys.argv[1]
_table = sys.argv[2]
_retry = 10
_wait = 60
#initialize ee
Config = ConfigParser.ConfigParser()
Config.read('.ee-properties')

MY_SERVICE_ACCOUNT = Config.get('Authentication', 'ServiceAccount')
MY_PRIVATE_KEY_FILE = Config.get('Authentication', 'PrivateKeyFile')
API_KEY = Config.get('Cartodb','APIKey')

ee.Initialize(ee.ServiceAccountCredentials(MY_SERVICE_ACCOUNT, MY_PRIVATE_KEY_FILE))
parks = ee.ImageCollection('GME/layers/04040405428907908306-05855266697727638016')

with open('data/' + _inputfile,'rb') as f:
    reader = csv.DictReader(f)
    recordNum = 1
    for row in reader:
        ee_id = row['ee_id']
        species = row['name']
        _range = ee.Image(ee_id)
        
        for i in range(0,_retry):
            try:
                print "#%s %s ee .map attempt #%s" % (recordNum,species,i)
                sumIntersect = parks.map(lambda image: rangeIntersect(image))
                numParks = sumIntersect.aggregate_count('intersect')
                inRange = sumIntersect.filter(ee.Filter.gt('intersect',0))
                numInRange = inRange.aggregate_count('intersect')
                print "%s numParks: %s, numParksInRange: %s" % (species, numParks.getInfo(),numInRange.getInfo())    
                
        #        print "%s" % inPark.getInfo()['features']
                
                count = 1
                
                for species in inRange.getInfo()['features']:
                    #p = species['properties']
                    print '#%s id: %s' % (count,species['id'])
                    saveRow(species)
                    count += 1
                      
                break;
            
            except:
                print sys.exc_info()[0]
                print traceback.format_exc()
                print "Waiting for %s seconds..." % _wait
                time.sleep(_wait)
        #end for
        recordNum+=1
    #end for
#end with



