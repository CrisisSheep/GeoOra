from facebook_scraper import get_posts
import json
from pathlib import Path
import pytz
import time
from datetime import datetime
import logging
from botocore.exceptions import ClientError
import boto3
from elasticsearch import Elasticsearch

# scraper config setup:
root_folder = Path(Path.cwd())
mapping_folder = Path(Path.cwd() / 'facebook_scraper' / 'mapping/')

with open(root_folder / 'facebook_scraper' / 'config.json', 'r') as f:
    config = json.load(f)

with open(root_folder / 'facebook_scraper' / 'credentials.json', 'r') as f:
    credentials = json.load(f)

facebook_config = config['facebook']
facebook_pages = facebook_config['pages']

#facebook_groups = facebook_config['groups']
facebook_dict_for_json = {} 
facebook_page_config_array = []
current_facebook_page_config = []
facebook_page_posts = []

#comprehend init
comprehend = boto3.client(service_name='comprehend', region_name='ap-southeast-2')

#Elasticsearch init
elastic = Elasticsearch(hosts=[credentials['elastic.url']], http_auth=(credentials['elastic.user'], credentials['elastic.password']), request_timeout=30)

tz = pytz.timezone("Pacific/Auckland")

post_id_dict = {}

def getComprehendAnalysis(text):
     if type(text) == str and len(text) > 5 and len(text) < 5000:
          result = comprehend.detect_sentiment(Text=text, LanguageCode='en')
          if result['ResponseMetadata']['HTTPStatusCode'] == 200:
               return {
                    "Sentiment": result['Sentiment'],
                    "SentimentScore": result['SentimentScore']
               }
          else:
               return None
     else:
          return None

def createElasticMapping(index):
     if not elastic.indices.exists(index):
          with open(mapping_folder / 'page_post.json', 'r') as f:
               mapping = json.load(f)

          # create an index with the mapping passed to the 'body' parameter
          response = elastic.indices.create(
               index=index,
               body=mapping,
               ignore=400
          )
          # print out the response:
          print ('response:', response)

          if 'acknowledged' in response:
               if response['acknowledged'] == True:
                    print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])

          # catch API error response
          elif 'error' in response:
               print ("ERROR:", response['error']['root_cause'])
               print ("TYPE:", response['error']['type'])

def savePagePost(post):
    result = elastic.index(index='facebook_post', body=post, id=post['post_id'])

createElasticMapping('page_post')

for page_config in facebook_pages:
     existing_post_ids = set(page_config['post_ids']) #used to check if post data has already been scraped
     new_post_ids = set() #to be compared with existing_post_ids

     for post in get_posts(page_config['id'], pages=facebook_config['max_limit'], timeout=30):
          #We want to check that each post has not been previously recorded

          # ensuring that each post has a timestamp associated with it
          try:
               print(post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat() or None)
          except:
               continue

          #TODO: Add the first post ID we come across to the new_post_ids, later store in post_ids
          #TODO: This way we can break when we come across the last post
          #TODO: This approach will allow us to just scrape new posts 
          #TODO: Scrape groups - Use date now as the time stamp once we're in synch

          #post id list for the current page
          new_post_ids.add(post["post_id"])

          #details to be added to the array of posts
          fb_post = {
               "post_id": post["post_id"],
               "text": post['text'],
               "post_text": post['post_text'],
               "shared_text": post['shared_text'],
               "timestamp": post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat() or None,
               "image": post['image'],
               "video": post['video'],
               "video_thumbnail":  post["video_thumbnail"],
               "likes": post["likes"],
               "comprehend": getComprehendAnalysis(post['text']),
               "reactions": post.get('reactions') or None,
               "group_id": page_config["id"],               
               "group_name": page_config["name"],
               "group_region": page_config["region"],
               "group_city": page_config["city"],
               "group_suburb": page_config["suburb"]
          }        
          facebook_page_posts.append(fb_post)
          savePagePost(fb_post)
          # Code for optimising periodic post id updates:
          if post["post_id"] in existing_post_ids:
               break
     
     current_facebook_page_config ={
          "name": page_config["name"],
          "id": page_config["id"],
          "region": page_config["region"],
          "city": page_config["city"],
          "suburb": page_config["suburb"],
          "posts": facebook_page_posts
     }

     #when all post ids have been added to new_post_ids set, we add the values to a dictionary
     post_id_dict[page_config['id']] = list(existing_post_ids.union(new_post_ids))
     
     facebook_page_config_array.append(current_facebook_page_config) 

     filename = str(page_config['id']) + '.json'
     filename = filename.lower()
     print(filename) #json filename for page data

     #creating json file for current facebook page
     with open(root_folder / 'facebook_scraper' / 'data' / filename, 'w') as outfile:
          tmp_dictionary = {
               "pages": current_facebook_page_config
          }
          json.dump(tmp_dictionary, outfile)

#setting up dictionary with all page information, to be used for main json file
facebook_dict_for_json = {
     "facebook": {
          "pages": facebook_page_config_array
     }
}

#when all the pages have been collected, we add every post id back into the config file..

#updating config
for page_config in facebook_config['pages']:
     if page_config['id'] in post_id_dict:
          page_config['post_ids'] = post_id_dict[page_config['id']]

#writing new config details to config json file     
with open(root_folder / 'facebook_scraper' / 'config.json', 'w') as outfile:
     json.dump(config, outfile)

#writing main page/post data json file
with open(root_folder / 'facebook_scraper' / 'facebook_posts.json', 'w') as outfile:
     json.dump(facebook_dict_for_json, outfile)

print('Scraping finished.')

#CODE FOR UPLOADING FILE TO S3 BUCKET
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

#upload_file("facebook_posts.json", "geoora")



# Code to add groups to the program in the future:

# for group in facebook_groups:
#      for post in get_posts(group=group['id'], pages=1):
#           #dt = post['time'].astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat()

#           fb_post = {
#                "post_id": post["post_id"],
#                "text": post['text'],
#                "post_text": post['post_text'],
#                "shared_text": post['shared_text'],
#                #"timestamp": post['time'].astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat(),
#                "image": post['image'],
#                "video": post['video'],
#                "video_thumbnail":  post["video_thumbnail"],
#                "likes": post["likes"] 
#           }        

#           facebook_posts.append(fb_post)
     
#      facebook_group ={
#           "name": group["name"],
#           "id": group["id"],
#           "region": group["region"],
#           "city": group["city"],
#           "suburb": group["suburb"],
#           "posts": facebook_posts
#      }