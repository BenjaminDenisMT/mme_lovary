import os
import logging
import requests
from datetime import date

from enum import Enum
from typing import Dict, Any
from dataclasses import dataclass
from requests.exceptions import RequestException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ShopifyApiVersion(Enum):
    V1 = "2019-04"
    V2 = "2019-07"
    V3 = "2019-10"
    V4 = "2020-01"
    V5 = "2020-04"

class ShopifyRequestType(Enum):
    inventory_level = "inventory_levels"
    shop_detail = "shop"
    product = "products"
    product_count = "products/count"


@dataclass
class ShopifyIdentification:
    """Generate the url needed by the Shopify API."""
    username: str
    password: str
    shop: str
    url_base: str = "myshopify.com/admin/api/"
    version: str = ShopifyApiVersion.V4.value


    def generate_url(self, request_type: ShopifyRequestType) -> str:
        url = f"https://{self.username}:{self.password}@{self.shop}.{self.url_base}{self.version}/{request_type.value}.json"
        logger.info(url)
        return url

        
@dataclass
class MmeLovary(ShopifyIdentification):
    username: str = 'de8a6e1d446aa624a581834ba8f8ee96'
    password: str = '8a5a8daec3cc832e23c43c1e0669d9ce'
    shop: str = "test-mme"


@dataclass
class Response:
    url: str
    request_parameters: Dict[str, Any] = None

    def send_request(self) -> Dict[str, str]:
        """Send a request and convert the response to a Json."""
        try:
            if not self.request_parameters:
                r = requests.get(self.url)
                logger.info(r.url)
            else:
                 r = requests.get(self.url, params=self.request_parameters)
                 logger.info(r.url)
                
            r.raise_for_status()

        except RequestException as e:
            raise ValueError(f"Request Exception cause by: {e}")
        
        return r.json()

def lambda_handler(event, context):
    product_ids = []
    request_product = Response(MmeLovary().generate_url(ShopifyRequestType.product)).send_request()
    for product in request_product['products']:
        for variants in product['variants']:
            if variants['inventory_item_id'] not in product_ids:
                product_ids.append(str(variants['inventory_item_id']))

    list_product_ids = {'inventory_item_ids': ','.join(product_ids)}
    request_inventory_level = Response(MmeLovary().generate_url(ShopifyRequestType.inventory_level), list_product_ids).send_request()

    for product_level in request_inventory_level['inventory_levels']:
        print(f"inventory_id: {product_level['inventory_item_id']}; inventory_level: {product_level['available']}; date: {product_level['updated_at']}, run_date: {date.today()}")

