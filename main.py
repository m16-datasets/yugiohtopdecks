import click
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import csv
import shutil
from collections import defaultdict
from slugify import slugify
from urllib.parse import urlparse
from kaggle import api
import json
import time
import logging
import datetime
import hashlib
import dateparser
import datetime

max_deck = 200000
max_failed = 10
base_url = "https://yugiohtopdecks.com/deck/{}"

html = Path("html")
html.mkdir(exist_ok=True)


def get_from_url(url):
    file_slug = slugify(url)

    file = Path(html, f"{file_slug}.html")
    if file.exists():
        with open(file) as r:
            content = r.read()
    else:
        response = requests.get(url)
        content = response.text
        with open(file, "w") as w:
            w.write(response.text)
            
    
    soup = BeautifulSoup(content, "lxml")
    deck_information = dict()
    metadata_container = [h3.parent for h3 in soup.find_all("h3") if h3.find("b") and h3.find("b").text != "Set Preferred Currency"][0]
    labels = metadata_container.find_all("b")
    contents = metadata_container.find_all("a")
    deck_name = labels[0].text.strip()
    deck_information["name"] = deck_name
    for label, content in zip(labels[1:], contents):
        slug = slugify(label.text.strip(": \n"), separator="_")
        deck_information[slug] = content.text.strip().replace(u'\xa0', u' ')

    card_listings = {h4.find("b").text: h4.parent for h4 in soup.find_all("h4") if h4.find("b")}
    if not card_listings:
        return None
    decks = ['Main', 'Extra', 'Side']
    obtained_deck = defaultdict(list)
    for deck in decks:
        slug = slugify(deck, separator="_")
        container = card_listings.get(deck + " Deck")
        if not container:
            continue
        ul = container.find("ul")
        for li in ul.find_all("li"):
            card_count = int(li.find("b").text.strip()[0])
            cards = [li.find("a").text.strip()] * card_count
            obtained_deck[slug].extend(cards)
    deck_information["deck"] = dict(obtained_deck)
    
    return deck_information

def setup_logger(log_file):
    logger = logging.getLogger("yugiohtopdecks")
    f_handler = logging.FileHandler(log_file)
    logger.setLevel(logging.INFO)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    return logger 


@click.command()
@click.argument("log_file", type=click.Path(dir_okay=False))
def main(log_file):
    logger = setup_logger(log_file)
    failed = 0

    decks = []

    for i in range(1, max_deck + 1):
        t0 = time.time()
        deck_information = get_from_url(base_url.format(i))
        t1 = time.time()
        time.sleep(max(t1 - t0), 0.5)
        if not deck_information:
            failed += 1
            if failed == max_failed:
                logger.warning(f"went over failed threshold at deck {i}")
                break
        else:
            decks.append(deck_information)
            failed = 0

    with open("data/decks.ndjson", "w") as writable:
        for deck in decks:
            json.dump(deck, writable)
            writable.write("\n")

    today = datetime.date.today().isoformat()
    api.dataset_create_version("data", f"Weekly dataset update ({today})")

if __name__ == "__main__":
    main()
