import csv
import pathlib
from xml.dom.minidom import parseString

import bs4

import requests


def getText(element, key):
    matches = element.getElementsByTagName(key)
    if not matches:
        return ''
    return matches[0].firstChild.data

def parseHtmlResults(s):
    bp = bs4.BeautifulSoup(s, 'lxml')
    table = bp.find('table')
    results = []
    for row in table.findAll('tr'):
        contents = []
        for cell in row.findAll('td'):
            contents.append(cell.text.strip())
        results.append(contents)
    return results

def parseFeed(xml_doc):
    for item in xml_doc.getElementsByTagName('item'):
        office = getText(item, 'title')
        date = getText(item, 'pubDate')
        description = getText(item, 'description')
        yield office, date, parseHtmlResults(description)


BASE_URL = 'https://electionreturns.pa.gov/electionFeed.aspx?ID='
REPORTS = [
    ('Primary', 25)
]
RAW_DATA = pathlib.Path('raw_data')
PARSED_DATA = pathlib.Path('data')

def scrape():
    RAW_DATA.mkdir(exist_ok=True)
    PARSED_DATA.mkdir(exist_ok=True)

    for report_name, feed_id in REPORTS:
        raw_folder = RAW_DATA / report_name
        report_folder = PARSED_DATA / report_name
        raw_folder.mkdir(exist_ok=True)
        report_folder.mkdir(exist_ok=True)

        url = BASE_URL + str(feed_id)
        res = requests.get(url)
        s = res.content.decode()
        xml_doc = parseString(s)
        last_build_date = getText(xml_doc, 'lastBuildDate')

        raw_filename = raw_folder / (last_build_date + '.csv')
        if not raw_filename.exists():
            with open(raw_filename, 'w') as f:
                f.write(s)

        for office, date, results in parseFeed(xml_doc):
            office_folder = report_folder / office
            office_folder.mkdir(exist_ok=True)
            parsed_filename = office_folder / (date + '.csv')
            if parsed_filename.exists():
                continue

            with open(parsed_filename, 'w') as f:
                out = csv.writer(f)
                out.writerows(results)
