import os
import logging
import requests
import psycopg2
from datetime import date
from datetime import datetime

from enum import Enum
from typing import Dict, Any, List
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
    report = "reports"
    order = "orders"


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
                # if r.headers()
                # print(r.headers)
                # link = "Link" next page link under the Link key
            else:
                r = requests.get(self.url, params=self.request_parameters)
                logger.info(r.url)

            r.raise_for_status()

        except RequestException as e:
            raise ValueError(f"Request Exception cause by: {e}")

        return r.json()


@dataclass
class RdsConnector:
    user: str = "data_lovary"
    password: str = "4lqRY0nAcWww"
    host: str = "mme-lovary.cluster-ro-ca6oh7xp1jbe.us-east-1.rds.amazonaws.com"
    port: str = "5432"
    database: str = "mme-lovary"

    def query_database(self, query: str) -> None:
        try:
            connection = psycopg2.connect(user=self.user, password=self.password, host=self.host, port=self.port, database=self.database)
            cursor = connection.cursor()
            cursor.execut(query)

        except psycopg2.Error as error:
            raise ValueError(f"Database error while connecting to RDS {error}")

        except Exception as error:
            raise ValueError(f"Error while connection to RDS {error}")

        finally:
            if connection:
                cursor.close()
                connection.close()
                logger.info("Connection closed")


def order_extract(extract_type: str = 'daily') -> List[Dict[str, str]]:
    order_list = []
    order_details = {}
    request_report = Response(MmeLovary().generate_url(ShopifyRequestType.order)).send_request()
    if extract_type == 'daily':
        for order in request_report['orders']:
            if not order['cancel_reason'] and not order['test'] and (datetime.strptime(order['updated_at'][:10], '%Y-%m-%d').date() == date.today()):
                for item in order['line_items']:
                    order_details["order_variant_id"] = item['variant_id']
                    order_details["order_title"] = item['title']
                    order_details["order_quantity"] = item['quantity']
                    order_details["order_sku"] = item['sku']
                    order_details["order_variant_title"] = item['variant_title']
                    order_details["order_name"] = item['name']
                    order_details["order_price"] = item['price']
                    order_details["order_total_discount"] = item['total_discount']
                    order_details["order_billing_address"] = order['billing_address']['province']
                    order_details["order_billing_country"] = order['billing_address']['country']
                    order_details["order_created_at"] = order['created_at']
                    order_details["order_updated_at"] = order['updated_at']
                    order_list.append(order_details)

    elif extract_type == 'all':
        if not order['cancel_reason'] and not order['test']:
            for item in order['line_items']:
                order_details["order_variant_id"] = item['variant_id']
                order_details["order_title"] = item['title']
                order_details["order_quantity"] = item['quantity']
                order_details["order_sku"] = item['sku']
                order_details["order_variant_title"] = item['variant_title']
                order_details["order_name"] = item['name']
                order_details["order_price"] = item['price']
                order_details["order_total_discount"] = item['total_discount']
                order_details["order_billing_address"] = order['billing_address']['province']
                order_details["order_billing_country"] = order['billing_address']['country']
                order_details["order_created_at"] = order['created_at']
                order_details["order_updated_at"] = order['updated_at']
                order_list.append(order_details)

    return order_list


def lambda_handler(event, context):
    order_to_sent = order_extract()
    print(order_to_sent)



if __name__ == "__main__":
    lambda_handler("1", "1")