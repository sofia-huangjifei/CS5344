#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import re
import json
import time
import hashlib
import random
from bs4 import BeautifulSoup
from urllib.request import urlopen


# In[2]:


IMAGEDIR='images'
PROFILES='scam'


# In[3]:


os.makedirs(IMAGEDIR, exist_ok=True)
os.makedirs(PROFILES, exist_ok=True)


# In[4]:


extractors = {
    'Username': re.compile(r'Username ([^\n]+)'),
    'Email': re.compile(r'Email ([^\n]+)'),
    'Name': re.compile(r'Name ([^\n]+)'),
    'Age': re.compile(r'Age ([^\n]+)'),
    'Purpose': re.compile(r'Here for ([^\n]+)'),
    'Looking': re.compile(r'Looking for ([^\n]+)'),
    'Location': re.compile(r'Location ([^\n]+)'),
    'Country': re.compile(r'Country\s*([^\n\r]+)'), 
    'City': re.compile(r'City\s*([^\n\r]+)'),   
    'Status': re.compile(r'Marital status ([^\n]+)'),
    'Children': re.compile(r'Children ([^\n]+)'),
    'Orientation': re.compile(r'Sexual Orientation ([^\n]+)'),
    'Ethnicity': re.compile(r'Ethnicity ([^\n]+)'),
    'Religion': re.compile(r'Religion ([^\n]+)'),
    'Occupation': re.compile(r'Occupation ([^\n]+)'),
    'Description': re.compile(r'Description\s*\n(.*)', re.DOTALL),
    'IP': re.compile(r'IP ([^\n]+)')
}


# In[5]:


def save_image(url):
    """ Take a URL, generate a unique filename, save the image to said file and return the filename."""
    ext = url.split('.')[-1]
    filename = IMAGEDIR+os.sep+hashlib.md5(url.encode('utf-8')).hexdigest()+'.'+ext
    if os.path.exists(filename):
        return filename
    try:
        content = urlopen(url).read()
        f = open(filename,'wb') 
        f.write(content)
        f.close()
    except:
        return None
    return filename 


# In[6]:


def extract_description(text):

    description_start = text.find("Description")
    if description_start != -1:
        # Extract content after 'Description'
        description_content = text[description_start + len("Description"):].strip()
        # Stop at 'IP address' or similar ending indicator
        ip_index = description_content.find("IP address")
        if ip_index != -1:
            description_content = description_content[:ip_index].strip()
        return description_content
    return None


# In[7]:


def scrape_profile(inhandle, outfile, year, month):
    html = inhandle.read()
    soup = BeautifulSoup(html, 'html.parser')

    content = soup.find('div', {'class': 'entry-content'})

    profiles = []
    profile_blocks = content.find_all(['p', 'hr'])
    
    profile = {}
    
    for block in profile_blocks:
        if 'More emails of scammers' in block.get_text():
            break
        if block.name == 'hr':
            if profile:
                profiles.append(profile)
                profile = {} 
            
        text = block.get_text(separator='\n').strip()

        img_tag = block.find_all('img')
        if img_tag:          
            profile['year_reported'] = year
            profile['month_reported'] = month
            profile['images'] = [save_image(img['src']) for img in img_tag]  
            
            # Initialize country_match and city_match
            country_match = None
            city_match = None
            
            for key, pattern in extractors.items():
                if key == 'Description':
                    continue  # Already handled
                match = pattern.search(text)
                if match:
                    if key == 'Country':
                        country_match = match.group(1).strip()
                    elif key == 'City':
                        city_match = match.group(1).strip()
                    else:
                        profile[key] = match.group(1).strip()
                        
                        
            location_match = extractors['Location'].search(text)
            if location_match:
                profile['Location'] = location_match.group(1).strip()
            elif country_match and city_match:
                profile['Location'] = f"{city_match}, {country_match}"
                    
            description_match = extractors['Description'].search(text)
            if description_match:
                description = description_match.group(1).strip()
                profile["Description"] = description
            else:
                print('****Description not found')
                
            
        # Check if IP was extracted
        ip_match = extractors['IP'].search(text)
        if ip_match:
            profile['IP'] = ip_match.group(1).strip()
            
            profiles.append(profile)
            profile = {}  

        # If the block is <hr>, it marks the end of a profile (secondary to IP logic)



    # Save output
    json.dump(profiles, open(outfile, 'w'))


# In[8]:


def enumerate_profiles(inhandle, page):
    html = inhandle.read()
    soup = BeautifulSoup(html, 'html.parser')
    
    profile_nodes = soup.findAll('h1', {'class': 'entry-title'})
    
    # Extract the href attribute from each <a> tag within those nodes
    urls = []
    for node in profile_nodes:
        link = node.find('a')
        if link and 'href' in link.attrs:
            urls.append(link['href'])
    
    print(urls)
    return urls


# In[9]:


def gather_all_profiles(year, month):
  """ Walk the index pages, harvesting the profile URLs, and then download and process all the profiles stored under this year and month. """
  page = 1
  urls = []

  print("{}-{} : Begin indexing.".format(year, month))

  while (page > 0):
    urlstring = "http://scamdigger.com/{}/{}/page/{}".format(year,month,page) 
    print(urlstring)
    jitter = random.choice([0,1])
    try:
      urlhandle = urlopen(urlstring)
      urls += enumerate_profiles(urlhandle, page)
      time.sleep(1+jitter)
      page += 1
    except:
      page = 0

  print("{}-{} : {} profiles".format(year,month,len(urls)))

  for url in urls:
    uid = url[30:-1]
    outfile=PROFILES+os.sep+uid+'.json'
    jitter = random.choice([0,1])
    try:
      urlhandle = urlopen(url)
      scrape_profile(urlhandle, outfile, year, month)
      time.sleep(1+jitter)
    except Exception as e:
      print("Exception when handling {}".format(url))
      print(e)
  
  print("{}-{} : complete.".format(year,month))


# In[10]:


def scrape(startyear, startmonth, endyear, endmonth):
  """ Walk the database through the defined ranges,downloading everything. """
  year = startyear
  month = startmonth
  while (not (year == endyear and month == endmonth)):
    ys = "{}".format(year)
    ms = "{:02d}".format(month)
    gather_all_profiles(ys,ms) 
    if month == 12:
      year += 1
      month = 0
    month += 1


# In[11]:


scrape(2024,3,2024,10)


# In[ ]:




