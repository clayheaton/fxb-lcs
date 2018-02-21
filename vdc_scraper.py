"""
Script that reads a list of links from a local SQLite
database and then scrapes the content tables from the VDC.
"""
from time import sleep
from bs4 import BeautifulSoup
import requests
import dataset
import json

# The last page of Arabic search results plus 1
IDX_END_AR = 1799


def get_content_table(link):
    """
    Scrapes a page, extracts the HTML table
    that has the interesting content,
    and returns it
    """
    result = requests.get(link)
    content = result.content
    soup = BeautifulSoup(content.decode(), "lxml")
    table = soup.find("table")

    if table is None:
        return table
    else:
        return str(table)


def extract_links_from_results_page(lang, idx, code):
    """
    Pulls the HTML from a search results page on the VDC and returns a list of links
    """
    links = []
    uri = "http://www.vdc-sy.info/index.php/" + lang + "/martyrs/" + str(idx) + "/" + code
    result = requests.get(uri)
    soup = BeautifulSoup(result.content,"lxml")
    for link in soup.find_all("a"):
        href = link.get("href")
        if "/details/martyrs/" in href:
            martyr_link = "http://www.vdc-sy.info" + href
            links.append(martyr_link)

    return links


def get_links_from_search(db, tab):
    """
    Scrapes the search results page to get the links that we
    need to scrape to get the entire data set.
    """
    code = "c29ydGJ5PWEua2lsbGVkX2RhdGV8c29ydGRpcj1ERVNDfGFwcHJvdmVkPXZpc2libGV8ZXh0cmFkaXNwbGF5PTB8"
    link_list_ar = []

    for num in range(1, IDX_END_AR):
        links = extract_links_from_results_page('ar', num, code)
        link_list_ar = link_list_ar + links
        sleep(1)
        if num % 100 == 0:
            print("Results page", num)    

        if num == IDX_END_AR - 1:
            print("LAST RESULTS PAGE:", num)

    print(len(link_list_ar), "LINKS HARVESTED")
    for link in link_list_ar:
        en_link = link.replace("/ar/", "/en/")
        rec = {"ar_link": link, "end_link": en_link}
        tab.insert(rec)

def main():
    """
    Scrape the VDC
    """

    db = dataset.connect("sqlite:///vdc.sqlite")
    tab = db['links']

    # Skip if we've already harvested links
    # HOWEVER we don't know how many to harvest
    # Check nohup.out to make sure we don't need to start again.
    if tab.count() == 0:
        # Populate the links table
        get_links_from_search(db, tab)

    # Explicitly create the tables

    tab_ar = db['content_ar']
    tab_en = db['content_en']

    rec = {"content":None, "url":None, "link_id": None, "lang": None, "success": None}

    tab_ar.insert(rec)
    tab_en.insert(rec)

    print(tab_ar.count())
    print(tab_en.count())

    tab_ar.delete()
    tab_en.delete()

    counter = 0

    # Since we record which links we've harvested,
    # this approach allows us to restart the script
    # in case it fails.
    recs = db.query("""SELECT * FROM links 
                    WHERE id NOT IN (
                        SELECT id FROM content_ar
                    );""")

    for rec in recs:
        counter += 1
        # Arabic
        try:
            content_table = get_content_table(rec['ar_link'])
            ar = {"content":content_table,
                  "url":rec['ar_link'],
                  "link_id":rec['id'],
                  "lang":"ar",
                  "success":1}
            tab_ar.insert(ar)
            sleep(1)
        except:
            ar = {"content":None,
                  "url":rec['ar_link'],
                  "link_id":rec['id'],
                  "lang":"ar",
                  "success":0}
            tab_ar.insert(ar)
            sleep(1)

        # English
        try:
            content_table = get_content_table(rec['en_link'])
            en = {"content":content_table,
                  "url":rec['en_link'],
                  "link_id":rec['id'],
                  "lang":"en",
                  "success":1}
            tab_en.insert(en)
            sleep(1)
        except:
            en = {"content":None,
                  "url":rec['en_link'],
                  "link_id":rec['id'],
                  "lang":"en",
                  "success":0}
            tab_en.insert(en)
            sleep(1)

        if counter % 100 == 0:
            sleep(2)

        if counter % 1000 == 0:
            print("Processed Link ID",rec['id'])

if __name__ == '__main__':
    main()
