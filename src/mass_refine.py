import ee
import ConfigParser
import time
import sys

def refine(sp):

    ### map method variable initialization
    
    scientificname = ee.String(sp.get('scientificname'))
    id = sp.get('range_id')
    range = ee.Image(rangeCollection.filterMetadata('system:index', 'equals', id).first())
    rangeMask = range.gte(0)
    prefs = ee.String(sp.get('updated_prefs')).split(',')

    forestCoverLow = ee.Number(sp.get('tree_cover_low')).multiply(100) #greater than this (units need to be *100 since data is 0-10,000)
    forestCoverHigh = ee.Number(sp.get('tree_cover_high')).multiply(100) #less than this (units need to be *100 since data is 0-10,000)
    
    elevationLow = ee.Number(sp.get('elev_min'))  #greater than
    elevationHigh = ee.Number(sp.get('elev_max'))  #less than
    
    ### end map method variable initialization
    
    #make the tree cover layer
    forestHab = _forest.gte(forestCoverLow).And(_forest.lte(forestCoverHigh)).select(["b1"],["habitat"])
    
    #make the elevation layer
    elevHab = _elev.gte(elevationLow).And(_elev.lte(elevationHigh)).select(["b1"],["habitat"])
    
    landcoverHab = ee.call('Image.select', binary_image, prefs).reduce(ee.Reducer.anyNonZero()).select(["any"], ["habitat"])

    #AND all the layers together
    habitat = ee.ImageCollection([forestHab, elevHab, landcoverHab]).reduce(ee.Reducer.allNonZero()).mask(rangeMask);
    
    area_image = habitat.eq(1).multiply(ee.Image.pixelArea());

    region = range.geometry();
    
    #Sum up the areas in the region of interest.
  
    area = area_image.reduceRegion(
                    reducer=ee.Reducer.sum(), 
                    geometry=region,
                    scale=1000,
                    maxPixels=10000000000,
                    bestEffort=True
                )
    
    f = ee.Feature(None).set('Scientificname', scientificname).set('Area',area);

    return f
# end refine function

####################
### Main Program ###
####################

_retry = 100
_wait = 300
#initialize ee
Config = ConfigParser.ConfigParser()
Config.read('.ee-properties')

MY_SERVICE_ACCOUNT = Config.get('Authentication', 'ServiceAccount')
MY_PRIVATE_KEY_FILE = Config.get('Authentication', 'PrivateKeyFile')

ee.Initialize(ee.ServiceAccountCredentials(MY_SERVICE_ACCOUNT, MY_PRIVATE_KEY_FILE))

modisCollection = ee.ImageCollection('GME/layers/04040405428907908306-05706461736190649367')
rangeCollection = ee.ImageCollection('GME/layers/04040405428907908306-05139691076993569618')

modis_list = [
  'GME/images/04040405428907908306-03680284325645907752',
  'GME/images/04040405428907908306-12923493437973401200',
  'GME/images/04040405428907908306-16898429718931540991',
  'GME/images/04040405428907908306-16395992718068236206',
  'GME/images/04040405428907908306-15358274840329672602',
  'GME/images/04040405428907908306-12698644595759757371',
  'GME/images/04040405428907908306-12477440792896460579',
  'GME/images/04040405428907908306-15698256553179772670',
  'GME/images/04040405428907908306-06780301927403350898',
  'GME/images/04040405428907908306-14332696666181097875',
  'GME/images/04040405428907908306-16447721153088195693',
  'GME/images/04040405428907908306-06713643388870702562',
  'GME/images/04040405428907908306-07640583113864798358',
  'GME/images/04040405428907908306-03630102749481522798',
  'GME/images/04040405428907908306-17836853509917069823',
  'GME/images/04040405428907908306-10234291591537811114',
  'GME/images/04040405428907908306-04479792456830051410'
]

binary_image = ee.Image().select([])

for loop in range(0,len(modis_list)):
    binary_image = binary_image.addBands(
        ee.Image(modis_list[loop]).select([0], [str(loop)]))

_forest = ee.Image('GME/images/04040405428907908306-09310201000644038383');
_elev = ee.Image('GME/images/04040405428907908306-08319720230328335274');

#species_prefs (initial 5 test species) - 1nfFpUT22C2_-F-8QqXiLNksP0CUTOB8sdyF9cfE2
#species_prefs_amr5 (5 species from amphibians, mammals, reptiles) - 1eLECiqYyrbpqKXx5IT2INq1YrVldEMN8KtZ2SabJ
#species_prefs_birds100 - 1BC4ZQBvpSv_KJLVaIed__Yk4aIsEL3k_bSIxAPEz
#species_prefs_birds1000 - 1tTp8-6xqmcQAtzxG_3WHqrs1wao_O8HgjsfknMnN
species = ee.FeatureCollection('ft:1tTp8-6xqmcQAtzxG_3WHqrs1wao_O8HgjsfknMnN')

for i in range(0,_retry):
    try:
        print "attempt #%s" % i
        results = species.map(lambda f: refine(f))
        features = results.getInfo()['features']

        count = 1
        for f in features:
            p = f['properties']
            print '#%s %s, Refined Area: %s' % (count,p['Scientificname'],p['Area']['habitat_all']/1000000)
            count += 1
            
        break;
    except:
        print sys.exc_info()[0]
        time.sleep(_wait)



