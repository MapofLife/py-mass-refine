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

    logging.info('%s Cartodb url: %s' % (dat['wdpa_id'],url))

    urllib2.urlopen(url)
    #print url

def saveToCSV(dat):
    with open("output/" + _table + ".csv", 'a') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',',
                                        quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                    writer.writerow([dat['wdpa_id'],
                                     dat['range_ee_id'],
                                     dat['wdpa_ee_id'],
                                     dat['intersect_area_km2']
                                     ])
                    
def rangeIntersect(collRast):

    intersection = _raster.gt(0).And(collRast.gt(0)) #raster on raster intersection
    #intersection = _range.clip(park) #feature on raster intersection
    
    intersectAreaImg = intersection.eq(1).multiply(ee.Image.pixelArea())
    
    area = intersectAreaImg.reduceRegion( #intersectAreaImg
                    reducer=ee.Reducer.sum(), 
                    geometry=intersectAreaImg.geometry(),
                    scale=1000,
                    maxPixels=10000000000,
                    bestEffort=True
                )

    #area is an object with a property called 'b1' that containst the value we want
    return ee.Feature(None)\
                .set('area_m2',area.get('b1'))\
                .set('ee_id',collRast.get('system:index'))
##### end rangeIntersect function

###
# Parks input csv file requires:
#    wdpa_id
#    wdpa_ee_id

####################
### Main Program ###
####################
_inputfile = sys.argv[1]
_table = sys.argv[2]
_retry = 3
_wait = 10
_save_cartodb = 1
_save_csv = 2
_save_method = _save_cartodb

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

bird_ranges  = 'GME/layers/04040405428907908306-05139691076993569618'
#amph range collection: 04040405428907908306-08271890370569606325
#mammals range collection: 04040405428907908306-17831395069316488559
#reptiles range collection: 04040405428907908306-00003475429954879412

### use for an image collection
collection = ee.ImageCollection(bird_ranges)\
            .map(lambda i: i.reproject("EPSG:4326", None, 1000))

### use for a fusion table of parks
# parks = ee.FeatureCollection("ft:1M26VibUh6o6ozJNc4fxli1N4eCXTY660A4DP6ehX")\
#            .map(lambda park: park.transform("EPSG:4326"))

scientificname = "undefined"
    
try:
    #loop through all items in the csv file.  use ee to make the intersection with the parks collection
    with open('data/' + _inputfile,'rb') as f:
        
        reader = csv.DictReader(f)
        recordNum = 1
        for row in reader:
            try:
                #use if looping through a csv of parks
                raster_id = row['wdpa_id']
                raster_ee_id = row['wdpa_ee_id']
                
                #use if looping through a csv of ranges
                #raster_id = row['scientificname'] 
                #raster_ee_id = row['TODO']
                
                ####use for testing a specific species or park
                #if scientificname != 'Leptotila rufaxilla': continue
                #if raster_ee_id != '04040405428907908306-03009765985537131020': continue
                ####use for testing a specific species
                
                msg = "#%s BEGIN: %s (%s) started processing" % (recordNum,raster_id,raster_ee_id)         
                print msg
                logging.info(msg)
                
                _raster = ee.Image('GME/images/' + raster_ee_id)\
                            .reproject("EPSG:4326", None, 1000)
                
                success = False
                
                #  put in a try catch block since ee may time out and we just need to try the request again
                for i in range(0,_retry):
                    try:
                        msg = "%s ee .map attempt #%s" % (raster_id,i)
                        print msg
                        logging.info(msg)
                        
                        #### .map method ####
#                         numIntersect = collection.filterBounds(_raster.geometry())\
#                                             .map(lambda collRast: rangeIntersect(collRast))#\
#                                             #.filter(ee.Filter.gt('area_m2',0))
#                         logging.info(numIntersect.getInfo())
#                         msg = "%s: Number of intersections: %s" % (raster_id, numIntersect.aggregate_count('area_m2').getInfo())
#                         print msg
#                        logging.info(msg)
#                        intersections = numIntersect.getInfo()['features']
                        #### end map method ####
                        
                        ####loop method ####
                        intersections = []
                        numIntersect = collection.filterBounds(_raster.geometry()).getInfo()

                        for collRast in numIntersect['features']:
                            
                            try:
                                collRast_ee_id = collRast['properties']['system:index']
                                logging.info("intersecting with: %s" % collRast_ee_id)
                                collRast = ee.Image('GME/images/'+collRast_ee_id)\
                                                .reproject("EPSG:4326", None, 1000)\
                                                .set('system:index',collRast_ee_id)
                                #if collRast_ee_id != '04040405428907908306-01758176619206754820':continue
                                intersection = rangeIntersect(collRast).getInfo()

                                area = intersection['properties']['area_m2']
                                logging.info('area: %s' % area)
                                if area > 0:
                                    intersections.append(intersection)

                            except Exception:
                                logging.error(sys.exc_info()[0])
                                logging.error(traceback.format_exc())

                        #### end loop method ####                        
                        
                        success = True
                        break;
                    
                    except Exception:
                        
                        logging.error(sys.exc_info()[0])
                        logging.error(traceback.format_exc())
                        logging.info("Waiting for %s seconds..." % _wait)
                        time.sleep(_wait)
                #end for
                        
                if not success:
                    msg = "FAILURE: %s Unable to perform refinement in ee" % raster_id
                    print msg       
                    logging.error(msg)            
                else:
                    
                    #now that we have the parks that intersect with the range, save the intersections to cartodb                    
                    count = 0
                    errors = 0            
                    for intersect in intersections:
                        try:
                            count += 1
                            dat = {}
                            #dat['scientificname'] = scientificname #use only if using a csv of species
                            dat['wdpa_id'] = row['wdpa_id']
                            #if using a csv of species, range_ee_id is returned in the intersect object
                            dat['range_ee_id'] = intersect['properties']['ee_id']
                            dat['wdpa_ee_id'] = row['wdpa_ee_id']
                            dat['intersect_area_km2'] = float(intersect['properties']['area_m2']) / 10**6 #convert to km2
                            saveRow(dat,_save_method)
                        except Exception:
                            errors += 1  
                            logging.error(sys.exc_info()[0])
                            logging.error(traceback.format_exc())
                            msg = '%s Unable to save record for park id: %s' % (scientificname,dat['wdpa_ee_id'])
                            print msg
                            logging.error(msg)
                            
                    #end for
                    if errors == 0:
                        msg = "SUCCESS: %s Saved %s out of %s records to datastore" % (raster_id,count,count) 
                    else:
                        msg = "FAILURE: %s Saved %s out of %s records to datastore" % (raster_id,count-errors,count)
                    
                    print msg
                    logging.info(msg)                        
                        
                #end else
                     
                recordNum+=1
            except Exception:
                msg = "FAILURE: %s Failed to process" % raster_id
                print msg
                logging.error(sys.exc_info()[0])
                logging.error(traceback.format_exc())
                logging.error(msg)
        #end for row in reader
    #end with
except Exception:
    msg = "%s Script Exited" % raster_id
    print msg
    logging.error(sys.exc_info()[0])
    logging.error(traceback.format_exc())
    logging.error(msg)
    




