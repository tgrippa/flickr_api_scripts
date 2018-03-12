# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 14:24:31 2018

@author: tais
"""

import flickrapi
import csv
import os, sys
import multiprocessing
from multiprocessing import Pool
import time


# Function for downloading locations info of 1 user id
def get_userinfo_singleuser(userid):
    import time
    return_list=[]
    query_succeed=False
    starttime=time.time()
    while query_succeed==False and (time.time()-starttime)<900: # If 15 minutes after first attempt to connect to Flicker's server
        try:
            #time.sleep(1)  #Wait for half second
            a=flickr.people.getInfo(api_key=api_key, user_id=userid, format='parsed-json')['person']
            query_succeed=True
        except:
            time.sleep(10) #Wait for ten seconds before retry
            continue
    return_list.append(a['id'].encode('ascii','ignore'))     # User ID
    return_list.append(a['username']['_content'].encode('ascii','ignore')) # User name
    return_list.append(a['profileurl']['_content'].encode('ascii','ignore')) # URL profile Flickr
    try:
        location=a['location']['_content']
        return_list.append(location.encode('ascii','ignore')) # User location information
    except:
        return_list.append("Nodata")
    return return_list

# Function to get location info for a list of user id
def get_userinfo_multipleuser(list_of_user, ncores=2):
    # Check for number of cores doesnt exceed available
    nbcpu=multiprocessing.cpu_count()
    if ncores>=nbcpu:
        ncores=nbcpu-1
    # Launch parallel computing
    p=Pool(ncores)
    returnlist=p.map(get_userinfo_singleuser,list_of_user) # the ordered results using map function
    p.close()
    p.join()
    # Return
    return returnlist


# Connect to Flickr API
global api_key
api_key = "4da87120db87b2b684d17dd5f5fa178d"
secret_api_key = "1f739d6180f0c63e"
flickr = flickrapi.FlickrAPI(api_key, secret_api_key)


'''
MAIN
'''

# Read the list of unique user ID from .csv
reader=csv.reader(open(outputcsv_photos,'r'),delimiter=',')
reader.next()
list_of_user=[]
[list_of_user.append(row[4]) for row in reader]
list_of_user=[x for x in set(list_of_user)] # Get unique values of user ID ising 'set'


a=time.time()
# Extract information of users and build content of the ouput .csv file
path,ext=os.path.splitext(outputcsv_photos)
outputcsv_users=path+"_users"+ext
content=[]
content=get_userinfo_multipleuser(list_of_user, ncores=10)  # Create content of the file
content.insert(0,['user_id','user_name','URL_profile','location']) # Insert header in first position
f=open(outputcsv_users,'w')
writer=csv.writer(f,delimiter=',')
writer.writerows(content)
f.close()

print "%.2f" %(time.time()-a)


