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

def saveRow(dat,method):
    if method == _save_cartodb:
        saveToCartodb(dat)
    elif method == _save_csv:
        saveToCSV(dat)
        
def saveToCartodb(dat):

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

    url = host + '?' + 'q=' + urllib.quote(sql,'') + '&api_key=' + API_KEY

    logging.info('%s Cartodb url: %s' % (dat['scientificname'],url))

    urllib2.urlopen(url)
    #print url

def saveToCSV(dat):
    with open("output/" + _table + ".csv", 'a') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',',
                                        quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                    writer.writerow([dat['scientificname'],
                                     dat['wdpaid'],
                                     dat['range_ee_id'],
                                     dat['park_ee_id'],
                                     dat['intersect_area_km2']
                                     ])
                    
def rangeIntersect(park):

    #intersection = _range.gt(0).And(park.gt(0)) #raster on raster intersection
    intersection = _range.clip(park) #feature on raster intersection
    
    intersectAreaImg = intersection.eq(1).multiply(ee.Image.pixelArea())
    
    area = intersectAreaImg.reduceRegion(
                    reducer=ee.Reducer.sum(), 
                    geometry=intersection.geometry(),
                    scale=1000,
                    maxPixels=10000000000,
                    bestEffort=True
                )
    #area is an object with a property called 'b1' that containst the value we want
    return ee.Feature(None)\
                .set('area_m2',area.get('b1'))\
                .set('wdpaid',park.get('wdpaid')) #only works if using fusion table with 
##### end refine function

####################
### Main Program ###
####################
_inputfile = sys.argv[1]
_table = sys.argv[2]
_retry = 3
_wait = 30
_save_cartodb = 1
_save_csv = 2

#initialize ee
Config = ConfigParser.ConfigParser()
Config.read('.ee-properties')
logging.basicConfig(filename='logs/runlog.txt',level=logging.DEBUG, filemode='w', datefmt='%Y-%m-%d %H:%M:%S')

MY_SERVICE_ACCOUNT = Config.get('Authentication', 'ServiceAccount')
MY_PRIVATE_KEY_FILE = Config.get('Authentication', 'PrivateKeyFile')
API_KEY = Config.get('Cartodb','APIKey')

ee.Initialize(ee.ServiceAccountCredentials(MY_SERVICE_ACCOUNT, MY_PRIVATE_KEY_FILE))

#wdpa_brazil = 'GME/layers/04040405428907908306-05855266697727638016' #25 parks in Brazil
#wdpa2014 = 'GME/layers/04040405428907908306-17831398992532573792' #the first 2131 parks

### use for an image collection of parks
#parks = ee.ImageCollection(wdpa_brazil)\
#            .map(lambda i: i.reproject("EPSG:4326", None, 1000))

### use for a fusion table of parks
parks = ee.FeatureCollection("ft:1M26VibUh6o6ozJNc4fxli1N4eCXTY660A4DP6ehX")\
            .map(lambda park: park.transform("EPSG:4326"))

scientificname = "undefined"
    
try:
    #loop through all species ranges.  use ee to make the intersection with the parks collection
    with open('data/' + _inputfile,'rb') as f:
        
        reader = csv.DictReader(f)
        recordNum = 1
        for row in reader:
            try:
                scientificname = row['scientificname']  
                
                ####use for testing a specific species
                if scientificname != 'Leptotila rufaxilla': continue
                ####use for testing a specific species
                          
                msg = "#%s BEGIN: %s started processing" % (recordNum,scientificname)         
                print msg
                logging.info(msg)
                
                range_ee_id = row['id']
                
                _range = ee.Image('GME/images/' + range_ee_id)\
                            .reproject("EPSG:4326", None, 1000)
                success = False;        
                
                #  put in a try catch block since ee may time out and we just need to try the request again
                for i in range(0,_retry):
                    try:
                        msg = "%s ee .map attempt #%s" % (scientificname,i)
                        print msg
                        logging.info(msg)
                        sumIntersect = parks.filterBounds(_range.geometry())\
                                            .map(lambda park: rangeIntersect(park)) #
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
                            dat['wdpaid'] = int(species['properties']['wdpaid'])
                            dat['range_ee_id'] = range_ee_id                            
                            dat['park_ee_id'] = species['id']
                            dat['intersect_area_km2'] = float(species['properties']['area_m2']) / 10**6 #convert to km2
                            saveRow(dat,_save_csv)                            
                        except:
                            errors += 1  
                            logging.error(sys.exc_info()[0])
                            logging.error(traceback.format_exc())
                            msg = '%s Unable to save record for park id: %s' % (scientificname,dat['park_ee_id'])
                            print msg
                            logging.error(msg)
                            
                    #end for
                    if errors == 0:
                        msg = "SUCCESS: %s Saved %s out of %s records to datastore" % (scientificname,count,count) 
                    else:
                        msg = "FAILURE: %s Saved %s out of %s records to datastore" % (scientificname,count-errors,count)
                    
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
    




