import datetime

import scrapy

from locations.geo import point_locations
from locations.hours import DAYS, OpeningHours
from locations.items import Feature


class TjxSpider(scrapy.Spider):
    name = "tjx"
    allowed_domains = ["tjx.com"]

    chains = {
        # USA chains
        "08": {"brand": "TJ Maxx", "brand_wikidata": "Q10860683", "country": "USA"},
        "10": {"brand": "Marshalls", "brand_wikidata": "Q15903261", "country": "USA"},
        # Canada chains
        "90": {"brand": "HomeSense", "brand_wikidata": "Q16844433", "country": "Canada"},
        "91": {"brand": "Winners", "brand_wikidata": "Q845257", "country": "Canada"},
        "93": {"brand": "Marshalls", "brand_wikidata": "Q15903261", "country": "Canada"},
        # These brands are pulled in separate spiders from separate websites that provide better data
        "28": {"brand": "Homegoods"},
        "50": {"brand": "Sierra"},
    }

    countries = {"Canada": "ca_centroids_100mile_radius.csv", "USA": "us_centroids_50mile_radius.csv"}

    def start_requests(self):
        for country, file in self.countries.items():
            chains = [k for k in self.chains if self.chains[k]["country"] == country]
            chains = str(chains).replace("[", "").replace("]", "").replace("'", "").replace(" ", "")
            for lat, lon in point_locations(file):
                yield scrapy.http.FormRequest(
                    url="https://marketingsl.tjx.com/storelocator/GetSearchResults",
                    formdata={
                        "chain": chains,
                        "lang": "en",
                        "maxstores": "100",
                        "geolat": lat,
                        "geolong": lon,
                    },
                    headers={"Accept": "application/json"},
                )

    def parse_hours(self, hours):
        try:
            """Mon-Thu: 9am - 9pm, Black Friday: 8am - 10pm, Sat: 9am - 9pm, Sun: 10am - 8pm"""
            opening_hours = OpeningHours()
            hours = hours.replace("Black Friday", "Fri")

            for x in hours.split(","):
                days, hrs = x.split(":", 1)
                try:
                    open_time, close_time = hrs.split("-")
                except:
                    continue

                if ":" in open_time:
                    open_time = datetime.datetime.strptime(open_time.strip(), "%I:%M%p").strftime("%H:%M")
                else:
                    open_time = datetime.datetime.strptime(open_time.strip(), "%I%p").strftime("%H:%M")

                if ":" in close_time:
                    close_time = datetime.datetime.strptime(close_time.strip(), "%I:%M%p").strftime("%H:%M")
                else:
                    close_time = datetime.datetime.strptime(close_time.strip(), "%I%p").strftime("%H:%M")

                if "-" in days:
                    start_day, end_day = days.split("-")
                    for day in DAYS[DAYS.index(start_day.strip()) : DAYS.index(end_day.strip()) + 1]:
                        opening_hours.add_range(day[:2], open_time=open_time, close_time=close_time)

                else:
                    day = days.strip()[:2]
                    opening_hours.add_range(day, open_time=open_time, close_time=close_time)

            return opening_hours.as_opening_hours()
        except:
            pass

    def parse(self, response):
        data = response.json()

        for store in data["Stores"]:
            properties = {
                "name": store["Name"],
                "ref": f'{store["Chain"]}{store["StoreID"]}',
                "addr_full": store["Address"].strip(),
                "city": store["City"],
                "state": store["State"],
                "postcode": store["Zip"],
                "country": store["Country"],
                "phone": store["Phone"],
                "lat": float(store["Latitude"]),
                "lon": float(store["Longitude"]),
                "brand": self.chains.get(store["Chain"], {}).get("brand"),
                "brand_wikidata": self.chains.get(store["Chain"], {}).get("brand_wikidata"),
            }

            hours = self.parse_hours(store["Hours"])
            if hours:
                properties["opening_hours"] = hours

            yield Feature(**properties)
