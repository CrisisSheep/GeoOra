from facebook_scraper import get_posts
import json
from pathlib import Path
import pytz
import time
from datetime import datetime
import logging
from botocore.exceptions import ClientError


# for post in get_posts(group="433563157024477"):
#      print(post['time'])

facebook = {}
facebook_page = []

root_folder = Path(Path.cwd())

with open(root_folder / 'facebook_scraper' / 'config.json', 'r') as f:
    config = json.load(f)

facebook_config = config['facebook']
facebook_pages = facebook_config['pages']
facebook_groups = facebook_config['groups']

facebook_posts = []
tz = pytz.timezone("Pacific/Auckland")

get_more_data = True
page_counter = 1

post_ids_dictionary = {}
for page in facebook_pages:
     existing_post_ids = set(page['post_ids'])
     new_post_ids = set()
     for post in get_posts(page['id'], pages=facebook_config['max_limit']):

          #We want to check that each post has not been previously recorded
          try:
               print(post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat() or None)
          except:
               continue
          
          new_post_ids.add(post["post_id"])

          if post["post_id"] in existing_post_ids:
               get_more_data = False
               continue

          fb_post = {
               "post_id": post["post_id"],
               "text": post['text'],
               "post_text": post['post_text'],
               "shared_text": post['shared_text'],
               "timestamp": post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat() or None,
               "image": post['image'],
               "video": post['video'],
               "video_thumbnail":  post["video_thumbnail"],
               "likes": post["likes"] 
          }        

          facebook_posts.append(fb_post)
     
     facebook_page ={
          "name": page["name"],
          "id": page["id"],
          "region": page["region"],
          "city": page["city"],
          "suburb": page["suburb"],
          "posts": facebook_posts
     }

     #when all post ids have been added to new_post_ids set, we add the values to a dictionary
     post_ids_dictionary[page['id']] = list(new_post_ids)

          

#when all the pages have been collected, we add every post id back into the config file..

for page in facebook_config['pages']:
     if page['id'] in post_ids_dictionary:
          page['post_ids'] = post_ids_dictionary[page['id']]

     
with open(root_folder / 'facebook_scraper' / 'config.json', 'w') as outfile:
     json.dump(config, outfile)

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


facebook = {
     "page": facebook_page
     #"group": facebook_page
}

with open(root_folder / 'facebook_scraper' / 'facebook_posts.json', 'w') as outfile:
     json.dump(facebook, outfile)

print('Hello')

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