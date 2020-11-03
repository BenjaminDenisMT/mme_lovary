import os
import logging
from enum import Enum
from datetime import date
from typing import Dict, Any, List
from datetime import datetime
from dataclasses import dataclass

import requests
import psycopg2
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
    order = "orders"
    locations = "locations"


@dataclass
class ShopifyIdentification:
    """Generate the url needed by the Shopify API."""
    shop: str
    url_base: str = "myshopify.com/admin/api/"
    version: str = ShopifyApiVersion.V4.value

    def generate_url(self, request_type: ShopifyRequestType) -> str:
        url = f"https://{self.shop}.{self.url_base}{self.version}/{request_type.value}.json"
        logger.info(url)
        return url


@dataclass
class MmeLovary(ShopifyIdentification):
    shop: str = "test-mme"


@dataclass
class Response:
    url: str
    request_parameters: Dict[str, Any] = None
    username: str = 'de8a6e1d446aa624a581834ba8f8ee96'
    password: str = '8a5a8daec3cc832e23c43c1e0669d9ce'

    def send_request(self) -> Dict[str, str]:
        """Send a request and convert the response to a Json."""

        response = True
        request_response = []
        while response:
            try:
                if not self.request_parameters:
                    r = requests.get(
                        self.url,
                        auth=(self.username, self.password)
                    )
                    logger.info(r.url)
                else:
                    r = requests.get(
                        self.url,
                        auth=(self.username, self.password),
                        params=self.request_parameters
                    )
                    logger.info(r.url)

                r.raise_for_status()

            except RequestException as e:
                raise ValueError(f"Request Exception cause by: {e}")

            if not r.headers.get('Link'):
                response = False

            elif r.links.get('next'):
                self.url = r.links['next']['url']
                self.request_parameters = None

            else:
                response = False

            call = r.json()
            request_response.append(call)

        return request_response


@dataclass
class RdsConnector:
    user: str = os.environ['user']
    credential: str = os.environ['credential']
    host: str = os.environ['host']
    port: str = os.environ['port']
    database: str = os.environ['database']

    def query_database(self, query: str) -> None:
        try:
            connection = psycopg2.connect(
                user=self.user,
                password=self.credential,
                host=self.host,
                port=self.port,
                database=self.database
            )
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()

        except psycopg2.Error as error:
            raise ValueError(f"Database error while connecting to RDS {error}")

        except Exception as error:
            raise ValueError(f"Error while connection to RDS {error}")

        finally:
            if connection:
                cursor.close()
                connection.close()
                logger.info("Connection closed")


def split_product_list(list_invenoty_id: List[str], value: int = 50):
    for i in range(0, len(list_invenoty_id), value):
        yield list_invenoty_id[i:i + value]


def lambda_handler(event, context):
    product_ids: List[str] = []
    locations_list: List[str] = []

    logger.info("Fetch iventory products ids")
    list_product = {'limit': '250'}
    request_product = Response(MmeLovary().generate_url(
        ShopifyRequestType.product), list_product).send_request()
    for i in request_product:
        for product in i['products']:
            for variants in product['variants']:
                if variants['inventory_item_id'] not in product_ids:
                    product_ids.append(str(variants['inventory_item_id']))

    logger.info("inventory store locations")
    request_locations = Response(MmeLovary().generate_url(
        ShopifyRequestType.locations)).send_request()
    for location in request_locations:
        for location_id in location["locations"]:
            locations_list.append(str(location_id["id"]))

    logger.info("Fetch inventory level")

    chunked_list = list(split_product_list(product_ids))
    for i in chunked_list:
        list_product_ids = {
            'inventory_item_ids': ','.join(i),
            'location_ids': ','.join(locations_list),
            'limit': '250'
        }
        request_inventory_level = Response(MmeLovary().generate_url(
            ShopifyRequestType.inventory_level), list_product_ids).send_request()
        logger.info("Sending inventory level to the database")

        for i in request_inventory_level:
            for product_level in i['inventory_levels']:
                insert_into = f"""
                INSERT INTO inventory_level (inventory_id, inventory_level, last_modification_time, run_date)
                values({product_level['inventory_item_id']}, {product_level['available']}, '{datetime.fromisoformat(product_level['updated_at'])}', '{date.today()}')
                """
                print(insert_into)
                RdsConnector().query_database(insert_into)


if __name__ == "__main__":
    lambda_handler(1, 2)