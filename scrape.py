import requests
import json
import pandas as pd
from sgzip.dynamic import DynamicGeoSearch, SearchableCountries

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

for search_lat, search_lon in search:
    url = "https://www.key.com/loc/DirectorServlet?action=getEntities&entity=BRCH&entity=ATM&entity=MCD&lat=" + str(search_lat) + "&lng=" + search_lon + "&distance=1000&callback=myJsonpCallback"

    response = requests.get(url).text
    response = response.replace("myJsonpCallback(", "")[:-1]
    response = json.loads(response)

    # print(len(response))
    # with open("file.txt", "w") as output:
    #     json.dump(response, output, indent=4)

    coords = []
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
                hours = loc_property["value"]

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

        current_coords = [latitude, longitude]
        coords.append(current_coords)
    search.mark_found(coords)
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

df = df.fillna("<MISSING>")
df = df.replace(r"^\s*$", "<MISSING>", regex=True)

df.to_csv("data.csv", index=False)