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
    username: str = "de8a6e1d446aa624a581834ba8f8ee96"
    password: str = "8a5a8daec3cc832e23c43c1e0669d9ce"

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


def order_extract(extract_type: str = 'daily') -> List[Dict[str, str]]:
    order_list = []
    header = {'limit': '100', 'status': 'any'}
    request_report = Response(MmeLovary().generate_url(ShopifyRequestType.order), header).send_request()
    previous_date = date.today() - timedelta(days=1)
    if extract_type == 'daily':
        for i in request_report:
            for order in i['orders']:
                if (not order['cancel_reason'] or order['cancel_reason'] == 'customer') and not order['test'] and (datetime.strptime(order['created_at'][:10], '%Y-%m-%d').date() == previous_date):
                    for item in order['line_items']:
                        order_details = {
                            "order_price": 0.0,
                            "order_shipping_price": 0.0,
                            "order_total_discount": 0.0,
                            "order_total_tax": 0.0
                        }
                        order_details['tags'] = order['tags'] if order['tags'] else 'None'
                        order_details['order_id'] = str(order['id']) + "|" + str(item['id'])
                        order_details["order_variant_id"] = item['variant_id']
                        order_details["order_title"] = item['title'].replace("'", "")
                        order_details["financial_status"] = order['financial_status']
                        order_details["order_quantity"] = item['quantity']
                        order_details["order_sku"] = item['sku']
                        order_details["order_variant_title"] = item['variant_title'].replace("'", "") if item['variant_title'] else 'None'
                        order_details["order_name"] = item['name'].replace("'", "")
                        if order['financial_status'] == 'refunded':
                            order_details["order_price"] += -float(item['price'])
                            if order['cancelled_at']:
                                order_details["order_created_at"] = order['cancelled_at']
                            else:
                                order_details["order_created_at"] = order['processed_at']
                        elif order['financial_status'] == 'pending':
                            order_details["order_price"] += float(item['price'])
                            order_details["order_created_at"] = order['processed_at']
                        elif order['financial_status'] == 'partially_refunded':
                            for refunds in order['refunds']:
                                if 'taxes' in refunds['note'].lower():
                                    for amount_refunded in refunds['order_adjustments']:
                                        if '-' in amount_refunded['amount']:
                                            order_details['order_total_tax'] += float(amount_refunded['amount']) / len(order['line_items'])
                                        else:
                                            order_details['order_total_tax'] += -float(amount_refunded['amount']) / len(order['line_items'])
                                else:
                                    for refunded_item in refunds['refund_line_items']:
                                        if item['id'] == refunded_item['line_item_id']:
                                            order_details["order_price"] += -refunded_item['subtotal']
                                            break

                            order_details["order_price"] += float(item['price'])
                            order_details["order_created_at"] = order['processed_at']
                        else:
                            order_details["order_price"] += float(item['price'])
                            order_details["order_created_at"] = order['created_at']
                        order_details["order_shipping_price"] += float(order['shipping_lines'][0]['price']) / len(order['line_items']) if len(order['shipping_lines']) else 0
                        order_details["order_total_discount"] += float(order["total_discounts"]) / len(order['line_items'])
                        order_details["order_total_tax"] += float(order["total_tax"]) / len(order['line_items'])
                        if 'billing_address' in order:
                            order_details["order_billing_address"] = order['billing_address']['province']
                            order_details["order_billing_country"] = order['billing_address']['country']
                        else:
                            order_details["order_billing_address"] = 'None'
                            order_details["order_billing_country"] = 'None'
                        order_details["order_updated_at"] = order['updated_at']
                        if order['source_name'] == 'web':
                            order_details["order_source_name"] = 'Online Store'
                        elif order['source_name'] == '580111':
                            order_details["order_source_name"] = 'Online Store'
                        elif order['source_name'] == 'pos':
                            order_details["order_source_name"] = 'Foire'
                        elif order['source_name'] == 'shopify_draft_order':
                            order_details["order_source_name"] = 'Distributeur'
                        order_list.append(order_details)

    elif extract_type == 'all':
        for i in request_report:
            for order in i['orders']:
                if (not order['cancel_reason'] or order['cancel_reason'] == 'customer') and not order['test'] and (datetime.strptime(order['created_at'][:10], '%Y-%m-%d').date() <= previous_date):
                    for item in order['line_items']:
                        order_details = {
                            "order_price": 0.0,
                            "order_shipping_price": 0.0,
                            "order_total_discount": 0.0,
                            "order_total_tax": 0.0
                        }
                        order_details['tags'] = order['tags'] if order['tags'] else 'None'
                        order_details['order_id'] = str(order['id']) + "|" + str(item['id'])
                        order_details["order_variant_id"] = item['variant_id']
                        order_details["order_title"] = item['title'].replace("'", "")
                        order_details["financial_status"] = order['financial_status']
                        order_details["order_quantity"] = item['quantity']
                        order_details["order_sku"] = item['sku']
                        order_details["order_variant_title"] = item['variant_title'].replace("'", "")
                        order_details["order_name"] = item['name'].replace("'", "")
                        if order['financial_status'] == 'refunded':
                            order_details["order_price"] += -float(item['price'])
                            if order['cancelled_at']:
                                order_details["order_created_at"] = order['cancelled_at']
                            else:
                                order_details["order_created_at"] = order['processed_at']
                        elif order['financial_status'] == 'pending':
                            order_details["order_price"] += float(item['price'])
                            order_details["order_created_at"] = order['processed_at']
                        elif order['financial_status'] == 'partially_refunded':
                            for refunds in order['refunds']:
                                if 'taxes' in refunds['note'].lower():
                                    for amount_refunded in refunds['order_adjustments']:
                                        if '-' in amount_refunded['amount']:
                                            order_details['order_total_tax'] += float(amount_refunded['amount']) / len(order['line_items'])
                                        else:
                                            order_details['order_total_tax'] += -float(amount_refunded['amount']) / len(order['line_items'])
                                else:
                                    for refunded_item in refunds['refund_line_items']:
                                        if item['id'] == refunded_item['line_item_id']:
                                            order_details["order_price"] += -refunded_item['subtotal']
                                            break

                            order_details["order_price"] += float(item['price'])
                            order_details["order_created_at"] = order['processed_at']
                        else:
                            order_details["order_price"] += float(item['price'])
                            order_details["order_created_at"] = order['created_at']
                        order_details["order_shipping_price"] += float(order['shipping_lines'][0]['price']) / len(order['line_items']) if len(order['shipping_lines']) else 0
                        order_details["order_total_discount"] += float(order["total_discounts"]) / len(order['line_items'])
                        order_details["order_total_tax"] += float(order["total_tax"]) / len(order['line_items'])
                        if 'billing_address' in order:
                            order_details["order_billing_address"] = order['billing_address']['province']
                            order_details["order_billing_country"] = order['billing_address']['country']
                        else:
                            order_details["order_billing_address"] = 'None'
                            order_details["order_billing_country"] = 'None'
                        order_details["order_updated_at"] = order['updated_at']
                        if order['source_name'] == 'web':
                            order_details["order_source_name"] = 'Online Store'
                        elif order['source_name'] == '580111':
                            order_details["order_source_name"] = 'Online Store'
                        elif order['source_name'] == 'pos':
                            order_details["order_source_name"] = 'Foire'
                        elif order['source_name'] == 'shopify_draft_order':
                            order_details["order_source_name"] = 'Distributeur'
                        order_list.append(order_details)

    return order_list


def lambda_handler(event, context):
    order_to_sent = order_extract()
    for i in order_to_sent:
        insert_into = f"""
        INSERT INTO daily_orders (order_id, variant_id, title, financial_status, quantity, sku, variant_title, name, price, order_shipping_price, total_discount, order_total_tax, province, country, created_at, updated_at, source_name, tags)
        values(
            '{i['order_id']}',
            '{i['order_variant_id']}',
            '{i['order_title'].replace("'", "")}',
            '{i['financial_status']}',
            {i['order_quantity']},
            '{i['order_sku']}',
            '{i['order_variant_title'].replace("'", "")}',
            '{i['order_name'].replace("'", "")}',
            {i['order_price']},
            {i['order_shipping_price']},
            {i['order_total_discount']},
            {i['order_total_tax']},
            '{i['order_billing_address']}',
            '{i['order_billing_country']}',
            '{i['order_created_at']}',
            '{i['order_updated_at']}',
            '{i['order_source_name']}'
            '{i['tags']}'
        )
        """
        print(insert_into)
        RdsConnector().query_database(insert_into)

if __name__ == "__main__":
    lambda_handler(1, 2)