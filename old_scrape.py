import csv
import json

from concurrent import futures
from lxml import html
from sgrequests import SgRequests
from sglogging import sglog

log = sglog.SgLogSetup().get_logger()


def write_output(data):
    with open("data.csv", mode="w", encoding="utf8", newline="") as output_file:
        writer = csv.writer(
            output_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL
        )

        headers = [
            "locator_domain",
            "page_url",
            "location_name",
            "street_address",
            "city",
            "state",
            "zip",
            "country_code",
            "store_number",
            "phone",
            "location_type",
            "latitude",
            "longitude",
            "hours_of_operation",
        ]
        writer.writerow(headers)

        for row in data:
            writer.writerow(row)


def generate_urls():
    urls = []
    session = SgRequests()
    r = session.get("https://www.key.com/locations/")
    tree = html.fromstring(r.text)
    states = tree.xpath("//ul[@class='grid__columns']/li/a/@href")
    for s in states:
        r = session.get(f"https://www.key.com{s}")
        tree = html.fromstring(r.text)
        cities = tree.xpath(
            "//ul[@class='alpha__list']//li/a[contains(@href, '/locations')]/@href"
        )
        for c in cities:
            c = f"https://www.key.com{c}"
            if c.count("/") == 7:
                urls.append(c)
                continue
            r = session.get(c)
            tree = html.fromstring(r.text)
            links = tree.xpath(
                "//article[contains(@class, 'card')]//a[contains(@href, 'locations/')]/@href"
            )
            for l in links:
                urls.append(f"https://www.key.com{l}")

    log.info("number of urls collected: " + str(len(urls)))
    return urls


def get_from_json(source):
    tree = html.fromstring(source)
    text = "".join(tree.xpath("//script[@type='application/ld+json']/text()"))
    try:
        j = json.loads(text)
    except:
        return []

    locator_domain = "https://key.com/"
    location_name = j.get("name") or "<MISSING>"
    log.info("location name: " + location_name)
    a = j.get("address") or {}
    street_address = a.get("streetAddress") or "<MISSING>"
    city = a.get("addressLocality") or "<MISSING>"
    state = a.get("addressRegion") or "<MISSING>"
    postal = a.get("postalCode") or "<MISSING>"
    country_code = "US"
    page_url = j.get("url") or "<MISSING>"

    if page_url:
        store_number = page_url.split("/")[-2]
    else:
        store_number = "<MISSING>"

    if page_url.lower().find("brch") != -1:
        location_type = "Branch"
    else:
        location_type = "Private Bank"
    phone = j.get("telephone") or "<MISSING>"
    g = j.get("geo") or {}
    latitude = g.get("latitude") or "<MISSING>"
    longitude = g.get("longitude") or "<MISSING>"

    _tmp = []
    hours = j.get("openingHoursSpecification") or []
    for h in hours:
        day = h.get("dayOfWeek").split("/")[-1] if h.get("dayOfWeek") else ""
        start = h.get("opens")
        if not start:
            _tmp.append(f"{day}: Closed")
        else:
            end = h.get("closes")
            _tmp.append(f"{day}: {start[:-3]} - {end[:-3]}")

    hours_of_operation = ";".join(_tmp) or "<MISSING>"

    if hours_of_operation.count("Closed") == 7:
        return []

    row = [
        locator_domain,
        page_url,
        location_name,
        street_address,
        city,
        state,
        postal,
        country_code,
        store_number,
        phone,
        location_type,
        latitude,
        longitude,
        hours_of_operation,
    ]

    return row


def get_data(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    session = SgRequests()
    r = session.get(url, headers=headers)

    log.info("targeted url: " + url)

    source = r.text
    row = get_from_json(source)

    return row


def fetch_data():
    out = []
    s = set()
    urls = generate_urls()

    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        tasks = {executor.submit(get_data, url): url for url in urls}
        for task in futures.as_completed(tasks):
            row = task.result()
            if row:
                line = tuple(row[2:6])
                if line not in s:
                    s.add(line)
                    out.append(row)

    return out


def scrape():
    data = fetch_data()
    write_output(data)


if __name__ == "__main__":
    scrape()
