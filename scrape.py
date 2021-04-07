from sgrequests import SgRequests
import json
import pandas as pd
from sgzip.dynamic import DynamicGeoSearch, SearchableCountries

session = SgRequests()

search = DynamicGeoSearch(country_codes=[SearchableCountries.USA])

locator_domains = []
page_urls = []
location_names = []
street_addresses = []
citys = []
states = []
zips = []
country_codes = []
store_numbers = []
phones = []
location_types = []
latitudes = []
longitudes = []
hours_of_operations = []


def getdata():
    x = 0
    for search_lat, search_lon in search:

        x = x + 1

        url = f"https://www.key.com/loc/DirectorServlet?action=getEntities&entity=BRCH&entity=MCD&lat={search_lat}&lng={search_lon}&distance=1000&callback=myJsonpCallback"

        response = session.get(url).text
        response = response.replace("myJsonpCallback(", "")[:-1]

        try:
            response = json.loads(response)
        except Exception:
            continue

        for location in response:
            locator_domain = "key.com"
            page_url = url

            location_properties = location["location"]["entity"]["properties"]
            for loc_property in location_properties:
                if loc_property["name"] == "LocationName":
                    location_name = loc_property["value"]

                if loc_property["name"] == "AddressLine":
                    address = loc_property["value"]

                if loc_property["name"] == "Locality":
                    city = loc_property["value"]

                if loc_property["name"] == "Subdivision":
                    state = loc_property["value"]

                if loc_property["name"] == "PostalCode":
                    zipp = loc_property["value"]

                if loc_property["name"] == "CountryRegion":
                    country_code = loc_property["value"]

                if loc_property["name"] == "LocationID":
                    store_number = loc_property["value"]

                if loc_property["name"] == "Phone1":
                    phone = loc_property["value"]

                if loc_property["name"] == "LocationType":
                    location_type = loc_property["value"]
                    if location_type == "BRCH":
                        location_type = "branch"
                    elif location_type == "ATM":
                        location_type = "ATM"
                    elif location_type == "MCD":
                        location_type = "Key Private Bank"
                    else:
                        location_type = "<MISSING>"

                if loc_property["name"] == "Latitude":
                    latitude = loc_property["value"]

                if loc_property["name"] == "Longitude":
                    longitude = loc_property["value"]

                if loc_property["name"] == "HoursOfOperation":
                    hours_list = loc_property["value"].replace(" - ", "-").split(" ")
                    x = 0
                    hour_values = []
                    days = []
                    for item in hours_list:
                        if x % 2 == 0:
                            hour_values.append(item)
                        else:
                            days.append(item)

                        x = x + 1

                    hours = ""
                    for index in range(len(days)):
                        hours = hours + days[index] + " " + hour_values[index] + ", "

            if zipp == "99999":
                zipp = "<MISSING>"

            if address == "Tbd":
                continue

            locator_domains.append(locator_domain)
            page_urls.append(page_url)
            location_names.append(location_name)
            street_addresses.append(address)
            citys.append(city)
            states.append(state)
            zips.append(zipp)
            country_codes.append(country_code)
            store_numbers.append(store_number)
            phones.append(phone)
            location_types.append(location_type)
            latitudes.append(latitude)
            longitudes.append(longitude)
            hours_of_operations.append(hours)

            search.found_location_at(latitude, longitude)


    df = pd.DataFrame(
        {
            "locator_domain": locator_domains,
            "page_url": page_urls,
            "location_name": location_names,
            "street_address": street_addresses,
            "city": citys,
            "state": states,
            "zip": zips,
            "store_number": store_numbers,
            "phone": phones,
            "latitude": latitudes,
            "longitude": longitudes,
            "hours_of_operation": hours_of_operations,
            "country_code": country_codes,
            "location_type": location_types,
        }
    )

    writedata(df)


def writedata(df):
    df = df.fillna("<MISSING>")
    df = df.replace(r"^\s*$", "<MISSING>", regex=True)

    df["dupecheck"] = (
        df["location_name"]
        + df["street_address"]
        + df["city"]
        + df["state"]
        + df["location_type"]
    )

    df = df.drop_duplicates(subset=["dupecheck"])
    df = df.drop(columns=["dupecheck"])
    df = df.replace(r"^\s*$", "<MISSING>", regex=True)
    df = df.fillna("<MISSING>")

    df.to_csv("data.csv", index=False)


getdata()
