import ee
import ConfigParser
import time
import sys
import traceback
import json

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

_retry = 10
_wait = 60
#initialize ee
Config = ConfigParser.ConfigParser()
Config.read('.ee-properties')

MY_SERVICE_ACCOUNT = Config.get('Authentication', 'ServiceAccount')
MY_PRIVATE_KEY_FILE = Config.get('Authentication', 'PrivateKeyFile')

ee.Initialize(ee.ServiceAccountCredentials(MY_SERVICE_ACCOUNT, MY_PRIVATE_KEY_FILE))

#ranges = ee.ImageCollection('GME/layers/04040405428907908306-05139691076993569618') #bird ranges
_range = ee.Image('GME/images/04040405428907908306-13291364293826882887')
parks = ee.ImageCollection('GME/layers/04040405428907908306-05855266697727638016')

for i in range(0,_retry):
    try:
        print "attempt #%s" % i
        sumIntersect = parks.map(lambda image: rangeIntersect(image))
        numParks = sumIntersect.aggregate_count('intersect')
        inRange = sumIntersect.filter(ee.Filter.gt('intersect',0))
        numInRange = inRange.aggregate_count('intersect')
        print "numParks: %s, numParksInRange: %s" % (numParks.getInfo(),numInRange.getInfo())    
        
#        print "%s" % inPark.getInfo()['features']
        
        count = 1
        
        for species in inRange.getInfo()['features']:
            #p = species['properties']
            print '#%s id: %s' % (count,species['id'])
            count += 1
              
        break;
    
    except:
        print sys.exc_info()[0]
        print traceback.format_exc()
        print "Waiting for %s seconds..." % _wait
        time.sleep(_wait)



