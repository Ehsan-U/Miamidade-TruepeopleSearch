import logging
from typing import Dict, Iterable, List
from urllib.parse import urlencode
import pandas as pd
import scrapy
from scrapy.http import Request, Response



class TruePeopleSearch(scrapy.Spider):
    name = "truepeoplesearch"
    remaining = None
    count = 0
    
    custom_settings = {
        "LOG_LEVEL": logging.INFO,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 503, 520],
        "CONCURRENT_REQUESTS": 32,
        "FEEDS": {"output.csv": {
                "format": "csv", 
                "overwrite": True,
                "fields": ["name", "age", "birth_year", "street", "city", "region", "zipcode", "phone-1", "phone-2", "phone-3", "phone-4", "phone-5",
                           "property_address","mailing_address","primary_land_use","actual_area","living_area","adjusted_area","market_value",
                           "assessed_value","year_built","actual_sqft","living_sqft","calc_value","tax_1_bill","tax_1_amount_due","tax_1_amount_paid",
                           "tax_1_status","tax_2_bill","tax_2_amount_due","tax_2_amount_paid","tax_2_status","tax_3_bill","tax_3_amount_due",
                           "tax_3_amount_paid","tax_3_status"]
            }
        }
    }

    def start_requests(self) -> Iterable[Request]:
        persons = self.load_input()
        self.remaining = len(persons)
        for person in persons:
            name, url = self.build_url(person)
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"name": name, "record": person['record']})


    def parse(self, response: Response, name: str, record: Dict):
        """
        parse the results page
        """
        self.count +=1
        self.logger.info(f" [+] Processed: {self.count}  Remaining: {self.remaining - self.count}")

        persons = response.xpath("//div[contains(@class, 'card-summary')]//div[contains(@class, 'hidden-mobile')]/a[contains(@href, '/find/person')]")
        if persons:
            for person in persons[:1]:
                link = person.xpath("./@href").get()
                url = response.urljoin(link)
                yield scrapy.Request(url, callback=self.parse_person, cb_kwargs={"record": record})
        else:
            self.logger.info(f"Skipped: {name}")
            yield None


    def parse_person(self, response: Response, record: Dict):
        """ 
        parse the person profile page 
        """
        item = {
            "name": response.xpath("//h1/text()").get(),
            "age": response.xpath("//span[contains(text(), 'Born')]/text()").re_first("(?:Age\s)(\d+)"),
            "birth_year": response.xpath("//span[contains(text(), 'Born')]/text()").re_first("\d{4}"),
            "street": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='streetAddress']/text()").get(),
            "city": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='addressLocality']/text()").get(),
            "region": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='addressRegion']/text()").get(),
            "zipcode": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='postalCode']/text()").get(),
            "phone-1": None,
            "phone-2": None,
            "phone-3": None,
            "phone-4": None,
            "phone-5": None,
            **record
        }

        phones = list(set(response.xpath("//span[@itemprop='telephone']/text()").getall()))
        for idx, phone in enumerate(phones, start=1):
            item[f"phone-{idx}"] = phone
            if idx == 5:
                break
        
        return item

    
    def load_input(self) -> List:
        queries = []
        df = pd.read_json("miamidade.json")
        for idx, row in df.iterrows():
            for owner in row.get("owners", []):
                record = row.to_dict()
                record.pop("owners")
                record = self.flatten(record)
                query = {
                    "name": owner,
                    "city": row['city'],
                    "state": row['state'],
                    "zipcode": '',
                    "record": record
                }
                queries.append(query)
        return queries
    

    @staticmethod
    def build_url(query):
        city_state = f"{query.get('city')}, {query.get('state')}"
        zipcode = query.get("zipcode")
        params = {"name":query.get("name"), "citystatezip": city_state or zipcode}
        url = f"https://www.truepeoplesearch.com/results?{urlencode(params)}"
        return (query.get("name"), url)
    

    def flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v, 1):
                    if isinstance(item, dict):
                        items.extend(self.flatten(item, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", item))
            else:
                items.append((new_key, v))
        return dict(items)