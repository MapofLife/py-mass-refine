import ee
import ConfigParser
import time
import sys
import traceback
import json

def rangeIntersect(image):
    masked = image.mask(_park);
    s = masked.reduceRegion(
                    reducer=ee.Reducer.sum(), 
                    geometry=_park.geometry(),
                    scale=1000,
                    maxPixels=10000000000,
                    bestEffort=True
                )
    
    return ee.Feature(None).set('intersect',s.get('b1')) #s is an object with a property called 'b1' that containst the value we want
##### end refine function

####################
### Main Program ###
####################

_retry = 10
_wait = 60
#initialize ee
Config = ConfigParser.ConfigParser()
Config.read('.ee-properties')

MY_SERVICE_ACCOUNT = Config.get('Authentication', 'ServiceAccount')
MY_PRIVATE_KEY_FILE = Config.get('Authentication', 'PrivateKeyFile')

ee.Initialize(ee.ServiceAccountCredentials(MY_SERVICE_ACCOUNT, MY_PRIVATE_KEY_FILE))

ranges = ee.ImageCollection('GME/layers/04040405428907908306-05139691076993569618') #bird ranges
_park = ee.Image('GME/images/04040405428907908306-07996474433989655713')

for i in range(0,_retry):
    try:
        print "attempt #%s" % i
        sumIntersect = ranges.map(lambda image: rangeIntersect(image))
        numRanges = sumIntersect.aggregate_count('intersect')
        inPark = sumIntersect.filter(ee.Filter.gt('intersect',0))
        numInPark = inPark.aggregate_count('intersect')
        print "numRanges: %s, numInPark: %s" % (numRanges.getInfo(),numInPark.getInfo())    
        
#        print "%s" % inPark.getInfo()['features']
        
        count = 1
        
        for species in inPark.getInfo()['features']:
            #p = species['properties']
            print '#%s id: %s' % (count,species['id'])
            count += 1
              
        break;
    
    except:
        print sys.exc_info()[0]
        print traceback.format_exc()
        print "Waiting for %s seconds..." % _wait
        time.sleep(_wait)



