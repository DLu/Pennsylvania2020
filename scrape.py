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


BASE_URL = 'https://electionreturns.pa.gov/electionFeed.aspx?ID=31'

RAW_DATA = pathlib.Path('raw_data')
PARSED_DATA = pathlib.Path('data')

def scrape():
    RAW_DATA.mkdir(exist_ok=True)
    PARSED_DATA.mkdir(exist_ok=True)

    res = requests.get(BASE_URL)
    s = res.content.decode()
    xml_doc = parseString(s)
    last_build_date = getText(xml_doc, 'lastBuildDate')

    raw_filename = RAW_DATA / (last_build_date + '.csv')
    if not raw_filename.exists():
        with open(raw_filename, 'w') as f:
            f.write(s)

    for office, date, results in parseFeed(xml_doc):
        office_folder = PARSED_DATA / office
        office_folder.mkdir(exist_ok=True)
        parsed_filename = office_folder / (date + '.csv')
        if parsed_filename.exists():
            continue

        other_existing = sorted(office_folder.glob('*.csv'))
        if other_existing:
            previous = other_existing[-1]
            previous_data = list(csv.reader(open(previous)))
            if previous_data == results:
                continue

        with open(parsed_filename, 'w') as f:
            out = csv.writer(f)
            out.writerows(results)
