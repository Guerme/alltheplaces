import datetime

import scrapy

from locations.geo import point_locations
from locations.hours import OpeningHours
from locations.items import Feature

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


class TjmaxxSpider(scrapy.Spider):
    name = "tjmaxx"
    allowed_domains = ["tjx.com"]

    chains = {
        "08": "TJ Maxx",
        "10": "Marshalls",
        "28": "Home Goods",
        "29": "Sierra",
        "50": "Home Sense",
    }
    wikidata = {
        "08": "Q10860683",
        "10": "Q15903261",
        "28": "Q5887941",
        "29": "Q7511598",
        "50": "Q16844433",
    }

    brand_chains_us = {"08": {"brand": "TJ Maxx", "Qcode": "Q10860683"},
                    "10": {"brand": "Marshalls", "Qcode": "Q15903261"},
                    "28": {"brand": "Home Goods", "Qcode": "Q5887941"},
                    "29": {"brand": "Sierra", "Qcode": "Q7511598"},
                    "50": {"brand": "Home Sense", "Qcode": "Q16844433"}}

    def start_requests(self):
        chains = str([k for k in self.brand_chains.keys()]).replace('[','"').replace(']','"').replace(' ','').replace("'","")
        self.logger.info(chains)
        for lat, lon in point_locations("us_centroids_50mile_radius.csv"):
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
                "brand": self.brand_chains_us.get(store["Chain"], {}).get("brand"),
                "brand_wikidata": self.brand_chains_us.get(store["Chain"], {}).get("Qcode"),
            }

            hours = self.parse_hours(store["Hours"])
            if hours:
                properties["opening_hours"] = hours

            yield Feature(**properties)
