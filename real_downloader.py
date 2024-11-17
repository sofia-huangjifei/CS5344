from gevent import monkey
monkey.patch_all()
import gevent
from gevent import Greenlet

import os
import re
import json
import time
import hashlib
import random
from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import traceback
from tqdm import tqdm
from tqdm.auto import trange

IMAGEDIR='images'
PROFILES='real'

iurlrx = re.compile('.* background-image: url\(([^\)]+)\)')

remap = {'I am' : 'gender',
         'Age' : 'age',
         'City' : 'location',
         'Marital status' : 'status',
         'Username' : 'username',
         'Ethnicity' : 'ethnicity',
         'Occupation' : 'occupation',
         'About me' : 'description',
         'My match\'s age' : 'match_age',
         'Children' : 'children',
         'Sexual Orientation' : 'orientation',
         'Religion' : 'religion',
         'Do you smoke' : 'smoking',
         'Do you drink' : 'drinking',
         'Here for' : 'intent',
         'Looking for': 'target'
         }

def save_image(url):
    """ Take a URL, generate a unique filename, save 
        the image to said file and return the filename."""
    ext = url.split('.')[-1]
    filename = IMAGEDIR+os.sep+hashlib.md5(url.encode('utf-8')).hexdigest()+'.'+ext
    if os.path.exists(filename):
        return filename
    try:
        content = urlopen(url).read()
        f = open(filename,'wb') 
        f.write(content)
        f.close()
    except e:
        print(e)
        return None
    return filename 


def scrape_profile(inhandle):
  """Scrape an input scamdiggers page for the profile content
  of the scammer. """
  #Read file
  html = inhandle.read()
  soup = BeautifulSoup(html, 'html.parser')

  pfnode = soup.find('div', {'class':'profile-BASE_CMP_UserViewWidget'})
  avnode = soup.find(id='avatar_console_image')

  #Pull the provided profile data out.
  rows = pfnode.findAll('tr')
  labels = {}
  for row in rows:
    lab = row.find('td',{'class':'ow_label'})
    val = row.find('td',{'class':'ow_value'})
    if lab:
      labels[lab.get_text()] = val.get_text().strip()

  profile = {}

  #Populate our own profile structure.
  for lab in remap:
    if lab in labels:
      profile[remap[lab]] = labels[lab]
    else:
      profile[remap[lab]] = "-"
  
  #Tweak for consistency.
  profile['gender'] = profile['gender'].lower()

  return profile
  
  #Extract avatar image
  # img = iurlrx.match(avnode.attrs['style']).group(1)
  # profile['images'] = [save_image(img)]

  #Save output
  # json.dump(profile, open(outfile,'w'))



def enumerate_profiles(inhandle):
  """ Extract all the profile page links from
  this index page. """
  html = inhandle.read()
  soup = BeautifulSoup(html, 'html.parser')
  
  urls = [ node.find('a')['href'] for node in soup.findAll('div',  {'class':'ow_user_list_data'})]
  return urls


def scrape(ch, thread_idx):
  """ Harvest profiles from every third page from the site. """
  urls = []
  urlstr="http://datingnmore.com/site/users/latest?page={}"

  # print("Begin URL harvesting.")

  #For every third page (sample size calculated to finish overnight). 
  # for i in tqdm(range(1, 2)): #3394):
  print("processing..", thread_idx)
  for m in trange(len(ch), desc="获取url - {}: ".format(thread_idx)):
  # for i in tqdm(range(ch[0], ch[-1]+1)): #3394):
    # print(thread_idx, ch[m])
    try:
      url = urlstr.format(ch[m])
      jitter = random.choice([0,1])
      urlhandle = urlopen(url)
      urls += enumerate_profiles(urlhandle)
      time.sleep(1+jitter)
    except Exception as e:
      print("获取url错误".format(traceback.format_exc()))
      print(ch, m)
      

  # print("Harvesting complete. {} URLs to scrape.".format(len(urls)))
  print("协程{} 获得url共: ".format(thread_idx), len(urls), ch[0], ch[-1])

  result = {
    v: [] for v in remap.values()
  }
  result["name"] = []

  for m in trange(len(urls), desc="解析结果 - {}: ".format(thread_idx)):
    url = urls[m]
  # for url in tqdm(urls):
    # for url in urls:
    # uid = url[33:]
    name = url.split("/")[-1]
    result["name"].append(name)
    # outfile=PROFILES+os.sep+uid+'.json'
    jitter = random.choice([0,1])
    try:
      urlhandle = urlopen(url)
      profile = scrape_profile(urlhandle)
      for k in remap.values():
        result[k].append(profile[k])
      time.sleep(1+jitter)
    except Exception as e:
      # print("Exception when handling {}".format(url))
      print("解析结果错误".format(traceback.format_exc()))
      for k in remap.values():
        result[k].append("error")

      # traceback.print_exc()
 
  # print("Scraping complete.")

  result_df = pd.DataFrame.from_dict(result)
  col_names = ["name"] + list(remap.values())
  result_df = result_df[col_names]

  # print("result..", ch[0], ch[-1], len(urls), len(result_df))
  

  # result_df.to_csv("./temp.csv", index=False)
  return result_df

def run(start, end):
  thread_cnt = 5
  idx_list = [i for i in range(start, end+1)]
  chunk_size = int(float(len(idx_list))/thread_cnt) + 1
  chunks = []
  for i in range(0, len(idx_list), chunk_size):
    chunks.append(idx_list[i:i+chunk_size])

  threads = []
  for i, ch in enumerate(chunks):
    t = Greenlet.spawn(scrape, ch, i)
    threads.append(t)
  gevent.joinall(threads)

  all_res_df = pd.concat([thread.value for thread in threads])
  all_res_df.to_csv("./res/result_{}_{}.csv".format(start, end), index=False)
  print("====FINISHED====")

if __name__ == "__main__":
  step = 500
  # for i in range(1, 3393, step): 
  # for i in range(1, 200, step): 
  for i in range(500, 3393, step): 
    end = min(i+step, 3393)
    print("====PROCESS====", i, end)
    run(i, end)

  # scrape([1, 3394])

  df = pd.read_csv("./res/result_1_501.csv")
  # df2 = pd.read_csv("./res/result_6_11.csv")
  # print(len(df))
  # print(len(set(df["name"].to_list() + df2["name"].to_list())))
  print(len(set(df["name"].to_list())))

