# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 14:24:31 2018
@author: Taïs Grippa
"""

import flickrapi
import csv
import os, time
import multiprocessing
from multiprocessing import Pool
from functools import partial
from shapely.geometry import box, Point, Polygon
import pandas as pd
import geopandas as gpd
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
import pyproj

'''
USER INPUTS
'''
## Output folder
outputfolder="PATH TO A FOLDER WHERE TO SAVE OUTPUTS"
## Input shapefile (should be WGS84 EPSG:4326)
inputshape="PATH TO THE SHAPEFILE OF THE AREA OF INTEREST"

## Connect to Flickr API
global api_key
api_key="ENTER_YOURS_HERE"
secret_api_key="ENTER_YOURS_HERE"
flickr=flickrapi.FlickrAPI(api_key, secret_api_key)

'''
FUNCTIONS
'''
# Function to get a specific page result from Flickr API for a specific BBOX
def get_pagenumber_result(bbox,pagenum):
    query_succeed=False
    starttime=time.time()
    while query_succeed==False and (time.time()-starttime)<600: # If 10 minutes after first attempt to connect to Flicker's server
        try:
            #time.sleep(1)  #Wait for half second
            page=flickr.photos.search(api_key=api_key, bbox=bbox,format='parsed-json', per_page=results_per_page, page=pagenum, extras='geo')
            query_succeed=True
        except:
            time.sleep(5) #Wait a bit before retry
            continue
    return page['photos']['photo']

# Function to get a result for multiple pages from Flickr API for a specific BBOX
def get_multiplepages_result(bbox,bboxtotalresult,ncores=2):
    # Create a list of page number to request
    number_page=bboxtotalresult/results_per_page+(1 if bboxtotalresult%results_per_page>0 else 0)
    list_pagenum=range(number_page+1)[1:]
    # Check for number of cores doesnt exceed available
    nbcpu=multiprocessing.cpu_count()
    if ncores>=nbcpu:
        ncores=nbcpu-1
    # Launch parallel computing
    p=Pool(ncores)
    func=partial(get_pagenumber_result,bbox)
    returnlist=p.map(func,list_pagenum) # the ordered results using map function
    p.close()
    p.join()
    # Return
    return returnlist

# Function saving info of photo for a single page of results
def get_photoinfo_singlepageresults(singlepageresult):
    return_list=[]
    for photo_dict in singlepageresult:
        current_row=[]
        current_row.append(photo_dict['id'])            # Photo ID
        current_row.append(photo_dict['latitude'])      # Location - Latitude
        current_row.append(photo_dict['longitude'])     # Location - Longitude
        current_row.append(photo_dict['accuracy'])      # Location - Accuracy
        current_row.append(photo_dict['owner'])         # User ID
        current_row.append(photo_dict['farm'])          # Farm
        current_row.append(photo_dict['server'])        # Server
        current_row.append(photo_dict['secret'])        # Secret
        photo_static_url='https://farm%s.staticflickr.com/%s/%s_%s.jpg'%(photo_dict['farm'],photo_dict['server'],photo_dict['id'],photo_dict['secret']) # Photo static URL
        current_row.append(photo_static_url)
        photo_flickr_website='https://www.flickr.com/photos/%s/%s'%(photo_dict['owner'],photo_dict['id'])  # Photo on Flickr website
        current_row.append(photo_flickr_website)
        return_list.append(current_row)
    return return_list

# Function saving info of photo for a multiple pages
def get_photoinfo_multiplepageresults(listofpages, ncores=2):
    # Check for number of cores doesnt exceed available
    nbcpu=multiprocessing.cpu_count()
    if ncores>=nbcpu:
        ncores=nbcpu-1
    # Launch parallel computing
    p=Pool(ncores)
    returnlist=p.map(get_photoinfo_singlepageresults,listofpages) # the ordered results using map function
    p.close()
    p.join()
    # Return
    return returnlist

# Function that check number of results for a bbox API request
def check_number_result_bbox(coord, accu=16):
    # Get number of photo and number of pages
    a=flickr.photos.search(api_key=api_key,
                           bbox=coord, format='parsed-json',
                           per_page=results_per_page, accuracy=accu)  # By default look for more accurately located information (could be not real accuracy according to my experience)
    total=int(a['photos']['total'])
    return total

# Function returning the area (squared meters) for a geometry provided in WGS84
def planar_area_from_wgs84_geom(geom):
    s = shape(geom)
    proj = partial(pyproj.transform, pyproj.Proj(init='epsg:4326'),
                   pyproj.Proj(init='epsg:3857'))
    s_new = transform(proj, s)
    projected_area = transform(proj, s).area
    return projected_area

'''
MAIN
'''
## Set projections definitions
proj4326={'init': 'epsg:4326'}
proj3857={'init': 'epsg:3857'}

## Define maximum number of result per page (250) and maximum results per request (4000)
global results_per_page, maxresult
results_per_page=250  #Max 250
maxresult=4000

## Import the Area Of Interest (AOI) polygon
aoi_gdf=gpd.read_file(inputshape)

#### Initial checking
# Check if outputfolder exists
if not os.path.exists(outputfolder):
    os.mkdir(outputfolder)
    print "The outputfolder <%s> didn't exist and just have been created."%outputfolder
# Check if AOI shapefile exists
if not os.path.isfile(inputshape):
    os.error("No file found on path <%s>."%inputshape)
# Check if CRS is EPSG:4326
if aoi_gdf.crs['init']!='epsg:4326':
    os.error("Input Shapefile's EPSG is not 4326.")
# Check if only one item in the shapefile
if len(aoi_gdf.index)!=1:
    os.error("The shapefile should contains exactly one item.")
# Check if only shapefile geometry is POLYGON
if str(aoi_gdf['geometry'][0])[:7] != "POLYGON":
    os.error("The shapefile geometry should be POLYGON.")

## Set up the initial BBox
minLon=float(aoi_gdf.bounds['minx'])
minLat=float(aoi_gdf.bounds['miny'])
maxLon=float(aoi_gdf.bounds['maxx'])
maxLat=float(aoi_gdf.bounds['maxy'])
global bbox_sizeok
initial_bbox=[minLon,minLat,maxLon,maxLat]
bbox_sizeok=[]
bbox_toolarge=[]
bbox_toolarge.append(initial_bbox)  # List which will contain the coordinates of bbox

## Print number of result in the initial BBox
coord="%s,%s,%s,%s"%(bbox_toolarge[0][0],bbox_toolarge[0][1],bbox_toolarge[0][2],bbox_toolarge[0][3])
nb=check_number_result_bbox(coord)
if nb < maxresult:
    print "There are %s results in the initial BBox."%nb
else:
    print "There are %s results in the initial BBox. It is too much for a single API request and the BBox will be sudivided (could take a while)."%nb

## Export Initial BBox as GeoJson for visualization in GIS
path_to_initial_bbox=os.path.join(outputfolder,"Initial_bbox.shp")
geom=[box(minLon,minLat,maxLon,maxLat)]
gdf=gpd.GeoDataFrame(crs=proj4326, geometry=geom)
gdf.to_file(path_to_initial_bbox)

## Subdivide the BBox if needed
loop_count=0
while len(bbox_toolarge)>0:
    newbboxes=[]
    loop_count+=1
    print "-------- Start bbox(es) subdivision loop number %s. --------"%loop_count
    print "Currently %s bbox(es) in the <bbox_sizeok> list."%len(bbox_sizeok)
    print "Currently %s bbox(es) in the <bbox_toolarge> list."%len(bbox_toolarge)
    for i,bbox in enumerate(bbox_toolarge):
        geombox=box(bbox[0],bbox[1],bbox[2],bbox[3]) #Geometry of the bbox (minLon,minLat,maxLon,maxLat)
        if gpd.GeoSeries(geombox)[0].disjoint(gpd.GeoSeries(aoi_gdf['geometry'])[0]):  # If the current bbox is completely outside the AOI, remove it
            bbox_toolarge.remove(bbox)
            continue # Leave the current loop and start the next iteration
        try:
            coord="%s,%s,%s,%s"%(bbox[0],bbox[1],bbox[2],bbox[3])
            total=check_number_result_bbox(coord)
        except:
            print "Check of number of result failed for item %s in the list. Retry in 5 seconds."%i
            time.sleep(5)  # Wait a bit before continuing
            continue # Leave the current loop and start the next iteration
        if total < maxresult:
            bbox.append(total)  # Save the number of result (< max) for this bbox (at index 4)
            bbox_sizeok.append(bbox)
            bbox_toolarge.remove(bbox)
        else:
            area=planar_area_from_wgs84_geom(geombox) #Get area in squared meters (EPSG:3857)
            density=total/float(area)
            if density < 500000:
                minLon=bbox[0]   # Current coordinates
                minLat=bbox[1]
                maxLon=bbox[2]
                maxLat=bbox[3]
                centLon=(minLon + maxLon)/2.0   # Average
                centLat=(minLat + maxLat)/2.0
                bbox_toolarge.remove(bbox)
                newbboxes.append([minLon, minLat, centLon, centLat]) #New bbox 1
                newbboxes.append([centLon, minLat, maxLon, centLat]) #New bbox 2
                newbboxes.append([minLon, centLat, centLon, maxLat]) #New bbox 3
                newbboxes.append([centLon, centLat, maxLon, maxLat]) #New bbox 4
            else:  # If photo density more than 1000000 per square meter
                print "Current bbox seems to be a black hole and will be removed from the list."
                bbox.append(-1)  # Save -1 as number of result to highlight it was a black hole
                bbox_sizeok.append(bbox)
                bbox_toolarge.remove(bbox)
    [bbox_toolarge.append(i) for i in newbboxes]

## Export divided BBox as shapefile
path_to_divided_bbox=os.path.join(outputfolder,"Divided_bbox.shp")
geom=[box(p[0],p[1],p[2],p[3]) for p in bbox_sizeok]
df=pd.DataFrame([p[4] for p in bbox_sizeok],columns=['nb_results'])  # Create a dataframe with number of result per bbox
gdf=gpd.GeoDataFrame(df, crs=proj4326, geometry=geom)
gdf.to_file(path_to_divided_bbox)

## Export Too Large BBox as shapefile
if len(bbox_toolarge)>0:
    path_to_toolarge_bbox=os.path.join(outputfolder,"Toolarge_bbox.shp")
    geom=[box(p[0],p[1],p[2],p[3]) for p in bbox_toolarge]
    gdf=gpd.GeoDataFrame(crs=proj4326, geometry=geom)
    gdf.to_file(path_to_toolarge_bbox)

## Get a list with all result pages for all bbox
listofpages=[]
for bbox in bbox_sizeok:
    coord="%s,%s,%s,%s"%(bbox[0],bbox[1],bbox[2],bbox[3])
    if bbox[4]>0: # Request API only if number of result for the BBOX is more than 0
        pages_current_bbox=get_multiplepages_result(coord,bbox[4],ncores=20)
        [listofpages.append(page) for page in pages_current_bbox]

## Extract information from the pages results and build content of the ouput .csv file
outputcsv_photos=os.path.join(outputfolder,"FlickR_points.csv")
content=[]
content=get_photoinfo_multiplepageresults(listofpages, ncores=5)  # Create content of the file
content=[a for sublist in content for a in sublist]  # Flat the results
content.insert(0,['id','latitude','longitude','accuracy','owner','farm','server','secret','URL_static','URL_website']) # Insert header in first position
f=open(outputcsv_photos,'w')
writer=csv.writer(f,delimiter=',')
writer.writerows(content)
f.close()

## Create Shapefile with results
path_to_results=os.path.join(outputfolder,"Flickr_photos.shp")
df=pd.read_csv(outputcsv_photos)  # Create a dataframe with number of result per bbox
geom=[Point(xy) for xy in zip(df.longitude, df.latitude)]
df=df.drop(['longitude', 'latitude'], axis=1)
gdf=gpd.GeoDataFrame(df, crs=proj4326, geometry=geom)
within_aoi=gpd.GeoSeries(gdf['geometry']).within(aoi_gdf.unary_union)
within_aoi=gpd.GeoSeries(gdf['geometry']).within(aoi_gdf.ix[0])
res_intersection=gdf[within_aoi] ## Keep only locations which intersects the AOI
res_intersection.to_file(path_to_results)