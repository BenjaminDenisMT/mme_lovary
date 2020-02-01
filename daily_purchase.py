import os
import logging
from enum import Enum
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, List
from datetime import date, timedelta

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
        response = True
        request_response = []
        auth = self.url[:74]
        while response:
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

            if not r.headers.get('Link'):
                response = False

            elif r.links.get('next'):
                url = r.links['next']['url'][8:]
                self.url = auth + url
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


def order_extract(extract_type: str = 'daily') -> List[Dict[str, str]]:
    order_list = []
    header = {'limit': '250', 'status': 'any'}
    request_report = Response(MmeLovary().generate_url(ShopifyRequestType.order), header).send_request()
    previous_date = date.today() - timedelta(days=1)
    if extract_type == 'daily':
        for i in request_report:
            for order in i['orders']:
                print(datetime.strptime(order['updated_at'][:10], '%Y-%m-%d').date() == previous_date)
                if not order['cancel_reason'] and not order['test'] and (datetime.strptime(order['created_at'][:10], '%Y-%m-%d').date() == previous_date):
                    for item in order['line_items']:
                        order_details = {}
                        order_details["order_variant_id"] = item['variant_id']
                        order_details["order_title"] = item['title'].replace("'", "")
                        order_details["order_quantity"] = item['quantity']
                        order_details["order_sku"] = item['sku']
                        order_details["order_variant_title"] = item['variant_title'].replace("'", "")
                        order_details["order_name"] = item['name'].replace("'", "")
                        order_details["order_price"] = item['price']
                        order_details["order_total_discount"] = float(order["total_discounts"]) / len(order['line_items'])
                        if 'billing_address' in order:
                            order_details["order_billing_address"] = order['billing_address']['province']
                            order_details["order_billing_country"] = order['billing_address']['country']
                        else:
                            order_details["order_billing_address"] = 'None'
                            order_details["order_billing_country"] = 'None'
                        order_details["order_created_at"] = order['created_at']
                        order_details["order_updated_at"] = order['updated_at']
                        order_details["order_source_name"] = order['source_name']
                        order_list.append(order_details)
                        order_list.append(order_details)

    elif extract_type == 'all':
        for i in request_report:
            for order in i['orders']:
                if not order['cancel_reason'] and not order['test'] and (datetime.strptime(order['created_at'][:10], '%Y-%m-%d').date() <= previous_date):
                    for item in order['line_items']:
                        order_details = {}
                        order_details["order_variant_id"] = item['variant_id']
                        order_details["order_title"] = item['title'].replace("'", "")
                        order_details["order_quantity"] = item['quantity']
                        order_details["order_sku"] = item['sku']
                        order_details["order_variant_title"] = item['variant_title'].replace("'", "")
                        order_details["order_name"] = item['name'].replace("'", "")
                        order_details["order_price"] = item['price']
                        order_details["order_total_discount"] = float(order["total_discounts"]) / len(order['line_items'])
                        if 'billing_address' in order:
                            order_details["order_billing_address"] = order['billing_address']['province']
                            order_details["order_billing_country"] = order['billing_address']['country']
                        else:
                            order_details["order_billing_address"] = 'None'
                            order_details["order_billing_country"] = 'None'
                        order_details["order_created_at"] = order['created_at']
                        order_details["order_updated_at"] = order['updated_at']
                        order_details["order_source_name"] = order['source_name']
                        order_list.append(order_details)

    return order_list


def lambda_handler(event, context):
    order_to_sent = order_extract()
    for i in order_to_sent:
        insert_into = f"""
        INSERT INTO daily_orders (variant_id, title, quantity, sku, variant_title, name, price, total_discount, province, country, created_at, updated_at, source_name)
        values(
            '{i['order_variant_id']}',
            '{i['order_title'].replace("'", "")}',
            {i['order_quantity']},
            '{i['order_sku']}',
            '{i['order_variant_title'].replace("'", "")}',
            '{i['order_name'].replace("'", "")}',
            {i['order_price']},
            {i['order_total_discount']},
            '{i['order_billing_address']}',
            '{i['order_billing_country']}',
            '{i['order_created_at']}',
            '{i['order_updated_at']}',
            '{i["order_source_name"]}'
        )
        """
        RdsConnector().query_database(insert_into)

