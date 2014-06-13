import ee
import ConfigParser
import time
import sys
import traceback
import json
import csv
import urllib
import urllib2
import logging

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
    apiKey = '6132d3d852907530a3b047336430fc1999eb0f24'
    url = host + '?' + 'q=' + urllib.quote(sql,'') + '&api_key=' + apiKey

    logging.info('%s Cartodb url: %s' % (dat['scientificname'],url))

    urllib2.urlopen(url)
    #print url
    
def rangeIntersect(park):
    intersection = _range.gt(0).mask(park.gt(0))
    #area_image = masked.eq(1).multiply(ee.Image.pixelArea())
    intersectAreaImg = intersection.eq(1).multiply(ee.Image.pixelArea())
    
    area = intersectAreaImg.reduceRegion(
                    reducer=ee.Reducer.sum(), 
                    geometry=park.geometry(),
                    scale=1000,
                    maxPixels=10000000000,
                    bestEffort=True
                )
    
    return ee.Feature(None).set('area_m2',area.get('b1')) #s is an object with a property called 'b1' that containst the value we want
##### end refine function

####################
### Main Program ###
####################
_inputfile = sys.argv[1]
_table = sys.argv[2]
_retry = 3
_wait = 30
#initialize ee
Config = ConfigParser.ConfigParser()
Config.read('.ee-properties')
logging.basicConfig(filename='logs/runlog.txt',level=logging.DEBUG, filemode='w', datefmt='%Y-%m-%d %H:%M:%S')

MY_SERVICE_ACCOUNT = Config.get('Authentication', 'ServiceAccount')
MY_PRIVATE_KEY_FILE = Config.get('Authentication', 'PrivateKeyFile')
API_KEY = Config.get('Cartodb','APIKey')

ee.Initialize(ee.ServiceAccountCredentials(MY_SERVICE_ACCOUNT, MY_PRIVATE_KEY_FILE))
parks = ee.ImageCollection('GME/layers/04040405428907908306-05855266697727638016')

scientificname = "undefined"

try:
    #loop through all species ranges.  use ee to make the intersection with the parks collection
    with open('data/' + _inputfile,'rb') as f:
        
        reader = csv.DictReader(f)
        recordNum = 1
        for row in reader:
            try:
                scientificname = row['name']  
                
                ####use for testing a specific species
                if scientificname != 'Synallaxis_gujanensis': continue
                ####use for testing a specific species
                          
                msg = "#%s BEGIN: %s started processing" % (recordNum,scientificname)         
                print msg
                logging.info(msg)
                
                range_ee_id = row['id']
                
                _range = ee.Image('GME/images/' + range_ee_id)
                success = False;        
                
                #  put in a try catch block since ee may time out and we just need to try the request again
                for i in range(0,_retry):
                    try:
                        msg = "%s ee .map attempt #%s" % (scientificname,i)
                        print msg
                        logging.info(msg)
                        sumIntersect = parks.map(lambda image: rangeIntersect(image))
                        numParks = sumIntersect.aggregate_count('area_m2')
                        inRange = sumIntersect.filter(ee.Filter.gt('area_m2',0))
                        numInRange = inRange.aggregate_count('area_m2')
                        msg = "%s: numParks: %s, numParksInRange: %s" % (scientificname, numParks.getInfo(),numInRange.getInfo())
                        print msg
                        logging.info(msg)
                        success = True
                        break;
                    
                    except:
                        
                        logging.error(sys.exc_info()[0])
                        logging.error(traceback.format_exc())
                        logging.info("Waiting for %s seconds..." % _wait)
                        time.sleep(_wait)
                #end for
                        
                if not success:
                    msg = "FAILURE: %s Unable to perform refinement in ee" % scientificname
                    print msg       
                    logging.error(msg)            
                else:
                    
                    #now that we have the parks that intersect with the range, save the intersections to cartodb                    
                    count = 0
                    errors = 0            
                    for species in inRange.getInfo()['features']:
                        try:
                            count += 1
                            dat = {}
                            dat['scientificname'] = scientificname
                            dat['range_ee_id'] = range_ee_id
                            dat['park_ee_id'] = species['id']
                            dat['intersect_area_km2'] = float(species['properties']['area_m2']) / 10**6 #convert to km2
                            saveRow(dat)                            
                        except:
                            errors += 1  
                            logging.error(sys.exc_info()[0])
                            logging.error(traceback.format_exc())
                            msg = '%s Unable to post record to cartodb for park id: %s' % (scientificname,dat['park_ee_id'])
                            print msg
                            logging.error(msg)
                            
                    #end for
                    if errors == 0:
                        msg = "SUCCESS: %s Posted %s out of %s records to cartodb" % (scientificname,count,count) 
                    else:
                        msg = "FAILURE: %s Posted %s out of %s records to cartodb" % (scientificname,count-errors,count)
                    
                    print msg
                    logging.info(msg)                        
                        
                #end else
                     
                recordNum+=1
            except:
                msg = "FAILURE: %s Failed to process" % scientificname
                print msg
                logging.error(sys.exc_info()[0])
                logging.error(traceback.format_exc())
                logging.error(msg)
        #end for row in reader
    #end with
except:
    msg = "%s Script Exited" % scientificname
    print msg
    logging.error(sys.exc_info()[0])
    logging.error(traceback.format_exc())
    logging.error(msg)
    




