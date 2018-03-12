# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 14:24:31 2018
@author: Taïs Grippa
"""

"""
TODO: 
    - Import initial shapefile with AOI + automatic compution of initial bbox (geopandas)
    - Filter the multiple bbox to be sure they all are in the AOI (not make API request for a bbox wich is completely outside the AOI)
    - Filter the final result (points) to keep only those inside the AOI
"""

import flickrapi
import csv
import os, time
import multiprocessing
from multiprocessing import Pool
from functools import partial 
from shapely.geometry import box, Point
import pandas as pd
import geopandas as gpd


'''
USER INPUTS
'''
## Connect to Flickr API
global api_key
api_key="ENTER_YOURS_HERE"
secret_api_key="ENTER_YOURS_HERE"
flickr=flickrapi.FlickrAPI(api_key, secret_api_key)

## Enter coordinates of the BBOX
#minLon=4.226303     # BBOX for Big Brussels
#minLat=50.769471
#maxLon=4.546094
#maxLat=50.958427
minLon=4.358697   # BBOX for Royal Palace of Brussels 
minLat=50.840067
maxLon=4.367709
maxLat=50.847654

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

def check_number_result_bbox(coord):
    # Get number of photo and number of pages
    a=flickr.photos.search(api_key=api_key,
                           bbox=coord, format='parsed-json',
                           per_page=results_per_page)
    total=int(a['photos']['total'])
    return total
    

'''
MAIN
'''
## Define maximum number of result per page (250) and maximum results per request (4000)
global results_per_page, maxresult
results_per_page=250  #Max 250
maxresult=4000

## Set up the initial BBox
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
path_to_initial_bbox=os.path.join("/Users/taisgrippa/Downloads","Initial_bbox.shp")
geom=[box(minLon,minLat,maxLon,maxLat)]
crs={'init': 'epsg:4326'}
df=pd.DataFrame([nb], columns=['nb_results'])  # Create a dataframe with number of result per bbox
gdf=gpd.GeoDataFrame(df, crs=crs, geometry=geom)
gdf.to_file(path_to_initial_bbox)

## Subdivide the BBox if needed
while len(bbox_toolarge)>0:
    for bbox in bbox_toolarge:
        coord="%s,%s,%s,%s"%(bbox[0],bbox[1],bbox[2],bbox[3])
        try:
            total=check_number_result_bbox(coord)
        except:
            time.sleep(5)  # Wait a bit before continuing
        if total < maxresult:
                bbox.append(total)  # Save the number of result (< max) for this bbox (at index 4)
                bbox_sizeok.append(bbox)
                bbox_toolarge.remove(bbox)
        else: 
            print "Going to subdivide the bbox"
            minLon=bbox[0]   # Current coordinates
            minLat=bbox[1]
            maxLon=bbox[2]
            maxLat=bbox[3]
            centLon=(minLon + maxLon)/2.0   # Average
            centLat=(minLat + maxLat)/2.0
            bbox_toolarge.append([minLon, minLat, centLon, centLat])
            bbox_toolarge.append([centLon, minLat, maxLon, centLat])
            bbox_toolarge.append([minLon, centLat, centLon, maxLat])
            bbox_toolarge.append([centLon, centLat, maxLon, maxLat])
            bbox_toolarge.remove(bbox)

## Export divided BBox as GeoJson for visualization in GIS
path_to_divided_bbox=os.path.join("/Users/taisgrippa/Downloads","Divided_bbox.shp")
geom=[box(p[0],p[1],p[2],p[3]) for p in bbox_sizeok]
crs={'init': 'epsg:4326'}
df=pd.DataFrame([p[4] for p in bbox_sizeok],columns=['nb_results'])  # Create a dataframe with number of result per bbox
gdf=gpd.GeoDataFrame(df, crs=crs, geometry=geom)
gdf.to_file(path_to_divided_bbox)

## Get a list with all result pages for all bbox
listofpages=[]
for bbox in bbox_sizeok:
    coord="%s,%s,%s,%s"%(bbox[0],bbox[1],bbox[2],bbox[3])
    pages_current_bbox=get_multiplepages_result(coord,bbox[4],ncores=2)
    [listofpages.append(page) for page in pages_current_bbox]


## Extract information from the pages results and build content of the ouput .csv file
outputcsv_photos=os.path.join("/Users/taisgrippa/Downloads","FlickR_points.csv")
content=[]
content=get_photoinfo_multiplepageresults(listofpages, ncores=5)  # Create content of the file
content=[a for sublist in content for a in sublist]  # Flat the results
content.insert(0,['id','latitude','longitude','accuracy','owner','farm','server','secret','URL_static','URL_website']) # Insert header in first position
f=open(outputcsv_photos,'w')
writer=csv.writer(f,delimiter=',')
writer.writerows(content)
f.close()

## Create Shapefile with results
path_to_results=os.path.join("/Users/taisgrippa/Downloads","flickr_photos.shp")
df=pd.read_csv(outputcsv_photos)  # Create a dataframe with number of result per bbox
df.longitude
geom=[Point(xy) for xy in zip(df.longitude, df.latitude)]
df=df.drop(['longitude', 'latitude'], axis=1)
crs={'init': 'epsg:4326'}
gdf=gpd.GeoDataFrame(df, crs=crs, geometry=geom)
gdf.to_file(path_to_results)
