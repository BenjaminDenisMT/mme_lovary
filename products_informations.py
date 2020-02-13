import os
import logging
from enum import Enum
from datetime import date
from typing import Dict, Any
from datetime import datetime
from dataclasses import dataclass

import requests
import psycopg2
from psycopg2 import sql
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
    locations = "locations"
    order = "orders"


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
    username: str = os.environ['username']
    password: str = os.environ['password']

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


def lambda_handler(event, context):
    products_information = []
    request_product = Response(MmeLovary().generate_url(ShopifyRequestType.product)).send_request()
    for i in request_product:
        for product in i['products']:
            for variants in product['variants']:
                products_information.append(
                    {variants['inventory_item_id']: {'product_name': product['title'], 'variants': variants['title']}}
                )
    for product_information in products_information:
        for product in product_information:
            insert_into = f"""
            INSERT INTO products_informations (inventory_id, product_name, variants)
            values({product}, '{product_information[product]['product_name'].replace("'", "")}', '{product_information[product]['variants']}')
            """
            RdsConnector().query_database(insert_into)