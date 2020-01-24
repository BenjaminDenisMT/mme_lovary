import os
import logging
from enum import Enum
from datetime import date
from typing import Dict, Any
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
    username: str = os.environ['username']
    password: str = os.environ['password']
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
    product_ids = []
    logger.info("Fetch iventory products ids")
    request_product = Response(MmeLovary().generate_url(
        ShopifyRequestType.product)).send_request()
    for product in request_product['products']:
        for variants in product['variants']:
            if variants['inventory_item_id'] not in product_ids:
                product_ids.append(str(variants['inventory_item_id']))

    list_product_ids = {'inventory_item_ids': ','.join(product_ids)}
    logger.info("Fetch iventory level")
    request_inventory_level = Response(MmeLovary().generate_url(
        ShopifyRequestType.inventory_level), list_product_ids).send_request()

    logger.info("Sending inventory level to the database")
    for product_level in request_inventory_level['inventory_levels']:
        insert_into = f"""
        INSERT INTO inventory_level (inventory_id, inventory_level, last_modification_time, run_date)
        values({product_level['inventory_item_id']}, {product_level['available']}, '{datetime.fromisoformat(product_level['updated_at'])}', '{date.today()}')
        """
        RdsConnector().query_database(insert_into)
