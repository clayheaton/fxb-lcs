from time import sleep
from bs4 import BeautifulSoup
import requests
import dataset
import json

def scrape_report(uri):
    result = requests.get(uri)
    
    # Make sure the server returned a good status code
    if result.status_code != 200:
        return None
    
    content = result.content
    soup = BeautifulSoup(content,"lxml")
    
    # Make sure we are not on a Page Not Found page
    if soup.find("h1").text == 'Page not found':
        return None

    # Establish the structure of the report
    report = {"title":"",
              "description":"", 
              "about":{},
              "link":uri}

    # Build the title of the resource
    report_title = soup.find("h1","restrict").text

    try:
        doc_extra_title = soup.find("h2","green").text
        report_title += " - " + doc_extra_title
    except Exception:
        pass

    report['title'] = report_title

    # Get data for "About" this resource
    resource_data = soup.find("table","resourcedata")
    table_cells = resource_data.find_all("td")
    for idx in range(0,len(table_cells)):
        if idx % 2 == 0:
            sec = table_cells[idx].text.replace("\xa0"," ").strip(":")
            val = table_cells[idx+1].text.replace("\xa0"," ")
            report["about"][sec] = val

    # Add the description
    resource_description = soup.find_all("p")[0].text.replace("\xa0"," ")
    report["description"] = resource_description

    return report

# Go through the search result pages for all articles going back to 2000.
# 142 pages as of 3 June 2017

# Harvest the report numbers from the search result pages
pages = 143
articles = []

for n in range(1,pages):
    uri = "http://www.syrialearning.org/resources.aspx?page="
    uri += str(n)
    uri += "&date=2000m1t2017m6"
    result = requests.get(uri)
    content = result.content
    soup = BeautifulSoup(content,"lxml")
    headers = soup.find_all("h4")
    for h in headers:
        try:
            num = h.find("a")['href'].split("/")[-1]
            title = h.find("a").text
            articles.append((num,title))
        except Exception:
            pass
    sleep(4)

# Scrape the report pages to gather the data we need to insert into Discourse
reports = []

for article in articles:
    uri_num = article[0]
    uri = "http://www.syrialearning.org/resource/" + uri_num
    
    try:
        rep = scrape_report(uri)
        if rep:
            reports.append(rep)
    except Exception:
        print("Failed",uri)
        
    sleep(8)

db = dataset.connect("sqlite:///article_metadata.sqlite")
tab_articles = db['articles']

fields = set()
for report in reports:
    about_keys = report["about"].keys()
    for key in about_keys:
        fields.add(key)
        
fields_lookup = {}

for field in fields:
    db_col = field.replace(" ","_").replace("(","").replace(")","").lower()
    fields_lookup[db_col] = field

for report in reports:
    try:
        title = report["title"]
    except:
        title = ""
        
    try:
        link = report["link"]
    except:
        link = ""
        
    try:
        description = report["description"]
    except:
        description = ""
        
    try:
        about = json.dumps(report["about"])
    except:
        about = ""
        
    record = {
        "title":title,
        "link":link,
        "description":description,
        "about":about
    }
    
    for db_col in fields_lookup.keys():
        try:
            record[db_col] = report["about"][fields_lookup[db_col]]
        except:
            record[db_col] = ""
    
    tab_articles.insert(record)


