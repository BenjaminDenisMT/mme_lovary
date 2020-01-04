import logging
import requests

from typing import ClassVar, Dict
from dataclasses import dataclass

logger = logging.getLogger("Shopify")

@dataclass
class ShopifyIdentification:
    """Generate the url needed by the Shopify API"""
    username: ClassVar[str]
    password: ClassVar[str]
    shop: ClassVar[str]

    def generate_url(self) -> str:
        url = f"https://{self.username}:{self.password}@{self.shop}.myshopify.com/admin/api/2020-01/shop.json"
        logger.info(url)
        return url

        
@dataclass
class MmeLovary(ShopifyIdentification):
    username: ClassVar[str] = "de8a6e1d446aa624a581834ba8f8ee96"
    password: ClassVar[str] = "8a5a8daec3cc832e23c43c1e0669d9ce"
    shop: ClassVar[str] = "test-mme"


@dataclass
class Response:
    url: ClassVar[str]

    def get_request(self) -> Dict[str, str]:
        """Send a request and convert the response to a Json"""
        r = requests.get(self.url).json()
        self._response_validation(r)

    @staticmethod
    def _response_validation(request: Dict[str, str]) -> None:
        """Validate if the https call received a status 200"""
        if not request.status_code == requests.codes.ok:
            raise request.raise_for_status()
        else:
            logger.info(f"Shopify request got a {requests.status_code}")



if __name__ == "__main__":
    Response("res")


