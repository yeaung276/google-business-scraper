import re
import csv
from urllib.parse import quote_plus, quote

import scrapy
import usaddress
from geopy.geocoders import Nominatim

search_keyword = "{keyword} {location} USA"
listings_url = (
    "https://www.google.com/localservices/prolist?g2lbs=AP8S6ENgyDKzVDV4oBkqNJyZonhEwT_VJ6_XyhCY8"
    "jgI2NcumLHJ7mfebZa8Yvjyr_RwoUDwlSwZt5ofLQk3D079b7a0tYFMAl-OvnNjzh2HzyjZNDGO0bloXZTJ8ttkCFt5r"
    "wXuqt_u&hl=en-PK&gl=pk&ssta=1&oq={q}&src=2&sa=X&scp=CgASABoAKgA%3D&q={q}&ved=2ahUKEwji7NSKjZ"
    "iAAxUfTEECHdJnDF8QjdcJegQIABAF&slp=MgBAAVIECAIgAIgBAJoBBgoCFxkQAA%3D%3D&lci={page}"
)
details_url = (
    "https://www.google.com/localservices/prolist?g2lbs=AP8S6ENgyDKzVDV4oBkqNJyZonhEwT_VJ6_XyhCY8"
    "jgI2NcumLHJ7mfebZa8Yvjyr_RwoUDwlSwZt5ofLQk3D079b7a0tYFMAl-OvnNjzh2HzyjZNDGO0bloXZTJ8ttkCFt5r"
    "wXuqt_u&hl=en-PK&gl=pk&ssta=1&oq={q}&src=2&sa=X&scp=CgASABoAKgA%3D&q={q}&ved=2ahUKEwji7NSKjZ"
    "iAAxUfTEECHdJnDF8QjdcJegQIABAF&slp=MgBAAVIECAIgAIgBAJoBBgoCFxkQAA%3D%3D&spp={id}"
)
us_states = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "tennsylvania": "PA",
    "thode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}
image_url = "https://www.google.com/_/AdsHomeservicesConsumerUi/data/batchexecute"
img_data = (
    "f.req=%5B%5B%5B%22wTe8We%22%2C%22%5B%5C%22{}%5C%22%2Cnull%2Cnull%2C%5C%22CgIgAQ%3D%3D%5C%22%2Cnull%2Cn"
    "ull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5C%22{}%5C%22%5D%5D%22%2Cnull%2C%22generic%22%5D%5"
    "D%5D&at=AAuQa1rFVkuavEc7pNrTSCWVJ59L%3A1702712109706&"
)


class GoogleBusinessSpider(scrapy.Spider):
    name = "google_businesses"
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "ZYTE_SMARTPROXY_ENABLED": True,
        "ZYTE_SMARTPROXY_URL": "api.zyte.com:8011",
        "ZYTE_SMARTPROXY_APIKEY": "bf34ea057aa340bfaf06eb0ecb05cc34",
        "ZYTE_SMARTPROXY_PRESERVE_DELAY": 0,
        "CONCURRENT_REQUESTS": 32,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 32,
        "CONCURRENT_ITEMS": 64,
        "AUTOTHROTTLE_ENABLED": False,
        "DOWNLOAD_TIMEOUT": 600,
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware": 610
        },
        "ITEM_PIPELINES": {
            "google_businesses.pipelines.GoogleBusinessesPipeline": 1,
        },
    }

    scraped_business = []

    def __init__(self):
        super(GoogleBusinessSpider, self).__init__()
        with open(f"input/input_keywords.csv", mode="r") as f:
            self.keywords = list(csv.DictReader(f))

    def start_requests(self):
        for location in ["Meryland"]:
            for keyword in self.keywords:
                query = search_keyword.format(
                    keyword=keyword["Keywords"], location=location
                )
                url = listings_url.format(q=quote_plus(query), page=0)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        "zyte_api_automap": {
                            "geolocation": "SE",
                            "responseCookies": True,
                        },
                    },
                    cb_kwargs={
                        "query": query,
                        "page": 0,
                        "keyword": keyword["Keywords"],
                    },
                )
    
    def parse(self, response, query, page, keyword):
        self.logger.info(
            f'Scraping {query} on page {page}... latency: {response.meta.get("download_latency", None)}'
        )
        for t in response.css(".AIYI7d::text"):
            print(t.get())

        # extract individual business details
        for listing_selector in response.css('div[jscontroller="xkZ6Lb"]'):
            item = {}
            listing_id = (
                listing_selector.css("::attr(data-profile-url-path)")
                .get("")
                .replace("/localservices/profile?spp=", "")
            )
            Name = listing_selector.css(".xYjf2e::text").get("").strip()
            Address = (
                listing_selector.css(".hGz87c span::text").getall()[-1]
                if listing_selector.css(".hGz87c span::text").getall()
                else ""
            )
            industry = (
                listing_selector.xpath(".//span[@class='hGz87c']/text()")
                .get("")
                .strip()
            )
            item["Industry"] = industry
            item["feature_id"] = listing_selector.css("::attr(data-feature-id)").get("")
            item["Name"] = Name
            item["Review_Type"] = (
                listing_selector.css("span.hGz87c::text").get("").strip()
            )
            detail_url = details_url.format(q=quote_plus(query), id=listing_id)
            if f"{Name} {Address}" not in self.scraped_business:
                yield scrapy.Request(
                    url=detail_url,
                    callback=self.extract_business_detail,
                    cb_kwargs={"business": item, "keyword": keyword},
                )
            else:
                self.logger.info(f"Already scraped {Name} {Address}")

        # next page
        if response.css('button[aria-label="Next"]'):
            url = listings_url.format(q=quote_plus(query), page=page + 20)
            meta = {
                "zyte_api_automap": {
                    "geolocation": "SE",
                    "responseCookies": True,
                },
            }
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta=meta,
                cb_kwargs={"query": query, "page": page + 20, "keyword": keyword},
            )

    def extract_business_detail(self, response, business, keyword):
        self.logger.info(
            f"Extracting details for {business['Name']}... latency: {response.meta.get('download_latency', None)}"
        )
        item = dict()
        item["Keyword"] = keyword
        item["Bussiness_Name"] = response.css("div.tZPcob::text").get("").strip()
        item["Bussiness_Contact"] = (
            response.css("div.eigqqc::text")
            .get("")
            .strip()
            .replace(" ", "")
            .replace("-", "")
            .replace("+", "")
            .replace(" ", "")
            .replace("-", "")
            .replace("(", "")
            .replace(")", "")
        )
        item["Business_URL"] = response.url
        item["Bussiness_Website"] = (
            response.css("a.iPF7ob::attr(href)")
            .get("")
            .strip()
            .replace("/url?sa=i&source=web&rct=j&", "")
            .replace("url=", "")
        )
        item["Bussiness_Service"] = response.xpath(
            '//*[contains(text(), "Services:")]/following::text()[1]'
        ).get("")
        item["Bussiness_Serving_Area"] = ", ".join(
            response.css("div.oR9cEb ::text").getall()
        )
        item["Industry"] = business.get("Industry")
        item["Address"] = response.css("div.fccl3c span::text").get("").strip()
        if item["Address"]:
            address = (
                item["Address"]
                .replace("USA", "United States")
                .replace("US", "United States")
            )
            try:
                parsed_address, _ = usaddress.tag(address)
                item["Street"] = (
                    parsed_address.get("AddressNumber", "")
                    + " "
                    + parsed_address.get("StreetName", ""),
                )
                item["City"] = "".join(list(parsed_address.get("PlaceName", [""])))
                item["State"] = "".join(list(parsed_address.get("StateName", [""])))
                item["Zipcode"] = "".join(list(parsed_address.get("ZipCode", [""])))
                item["Country"] = parsed_address.get("CountryName", "")
                if not item["State"]:
                    for i in us_states.keys():
                        if i in address.lower():
                            item["State"] = us_states[i]
                        if (
                            "united states" in address.lower()
                            or "usa" in address.lower()
                            or "us" in address.lower()
                        ):
                            item["Country"] = "United States"
                else:
                    address = (
                        address.replace(item["City"], "")
                        .replace(item["State"] + " ", "")
                        .replace(item["Zipcode"], "")
                        .replace(item["Country"], "")
                        .replace(",", "")
                    )
                    item["Street"] = address.strip()
            except Exception as e:
                self.logger.error(f"Error parsing {business['Name']}. {e}")
                item["Street"] = ""
                item["City"] = ""
                item["State"] = ""
                item["Zipcode"] = ""
                item["Country"] = ""

        item["Description"] = response.css("h3.NwfE3d+div::attr(data-long-text)").get(
            ""
        )
        item["Review_Type"] = business.get("Review_Type")
        item["Google_Map_Link"] = (
            response.css('a[aria-label="Directions"]::attr(href)').get("").strip()
        )
        sttr = '//script[contains(text(),"hash: {}")]/text()'.format("'3'")
        script = response.xpath(sttr).get("{}")
        days = [
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
            "Monday",
        ]
        opening_time = []
        found = False
        for day in days:
            match = re.search(f'"{day}",(.*?)false', script)
            if match:
                found = True
                hours = match.group(1).split('[["')[-1].split('"')[0]
                opening_time.append(f"{day}: {hours}")
        if not found:
            sttr = '//script[contains(text(),"hash: {}")]/text()'.format("'4'")
            script = response.xpath(sttr).get("{}")
            days = [
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
                "Monday",
            ]
            opening_time = []
            for day in days:
                match = re.search(f'"{day}",(.*?)false', script)
                if match:
                    hours = match.group(1).split('[["')[-1].split('"')[0]
                    opening_time.append(f"{day}: {hours}")
        item["Opening Hours"] = " / ".join(opening_time)
        feature_id = business.get("feature_id")
        data = img_data.format(quote(feature_id), quote(feature_id))
        yield scrapy.Request(
            image_url,
            headers={
                "accept": "*/*",
                "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            },
            body=data,
            method="POST",
            callback=self.extract_image,
            errback=self.error_fallback,
            cb_kwargs={"business": item},
        )

    def extract_image(self, response, business):
        data = str(response.body)
        pattern = r"https://lh5\.googleusercontent\.com/p.*?(?=\\\\)"
        matches = re.findall(pattern, data)
        business["Images"] = ", ".join(matches)
        latitude, longitude = self.get_lat_lng(business["Address"])
        if latitude and longitude:
            business["Lat"] = latitude
            business["Lon"] = longitude
            yield business
        else:
            if business["Google_Map_Link"]:
                yield scrapy.Request(
                    url=business["Google_Map_Link"],
                    callback=self.extract_lon_lat,
                    errback=self.error_fallback,
                    cb_kwargs={"business": business},
                )
            else:
                yield business

    def extract_lon_lat(self, response, business):
        values = (
            response.xpath("//script/text()")
            .re_first("window.APP_INITIALIZATION_STATE=(.*),")
            .split("]", 1)[0]
            .split(",")[1:]
        )
        business["Lat"] = values[0]
        business["Lon"] = values[-1]
        yield business

    def get_lat_lng(self, address):
        try:
            geolocator = Nominatim(user_agent="my_geocoder", timeout=30)
            location = geolocator.geocode(address)
            if location:
                return location.latitude, location.longitude
            else:
                return None, None
        except Exception as e:
            print(f"Exception Occur for lat, Long: {e}")
            return None, None

    def error_fallback(self, failure):
        self.logger.error('error happend')
        return failure.request.cb_kwargs.get("business", {})
