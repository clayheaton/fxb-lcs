"""
Script that reads a list of links from a local SQLite
database and then scrapes the content tables from the VDC.
"""
from time import sleep
from bs4 import BeautifulSoup
import requests
import dataset
import json


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


def main():
    """
    Scrape the VDC
    """
    db = dataset.connect("sqlite:///vdc.sqlite")
    tab = db['links']
    tab_ar = db['content_ar']
    tab_en = db['content_en']

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
