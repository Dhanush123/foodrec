import os
from pathlib import Path
import random
import time
from typing import Dict, List, NamedTuple, Optional
import csv
import concurrent.futures

from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from utils import GracefulKiller

BASE_UE_URL = "https://www.ubereats.com"
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
}


class RestaurantInfo(NamedTuple):
    name: str
    rating: float
    rel_url: str
    menu: Optional[Dict[str, str]] = None


class CategoryInfo(NamedTuple):
    name: str
    rel_url: str


class ItemInfo(NamedTuple):
    name: str
    description: str


def parse_city(city: str) -> str:
    return f"{city.replace(' ', '-').lower()}-ca"


def get_city_url(city: str) -> str:
    city = city.replace(" ", "-").lower()
    url = f"{BASE_UE_URL}/city/{city.lower()}-ca"
    return url


def get_rating_from_restaurant_box(restaurant_box: BeautifulSoup) -> int:
    for child in restaurant_box.findChildren("div", recursive=True):
        try:
            rating = float(child.text)
            return rating
        except Exception:
            continue
    return 0


def get_restaurants_in_category(
    category: CategoryInfo, restaurants_limit: Optional[int]
) -> List[RestaurantInfo]:
    # TODO: filter out restaurants that are too far for delivery
    category_res = requests.get(
        f"{BASE_UE_URL}{category.rel_url}", headers=BASE_HEADERS
    )
    page_info = BeautifulSoup(category_res.text, features="html.parser")

    info_list = []
    enumerated = 0
    for header in tqdm(page_info.find_all("h3")):
        if restaurants_limit is not None and enumerated == restaurants_limit:
            break
        try:
            rel_restaurant_url = header.parent.get("href")
            # Some urls might not be a restaurant
            if rel_restaurant_url.startswith("/store"):
                restaurant_name = header.get_text()
                rating = get_rating_from_restaurant_box(header.parent.parent)
                info_list.append(
                    RestaurantInfo(restaurant_name, rating, rel_restaurant_url)
                )
                enumerated += 1
        except Exception:
            continue
    return info_list


def get_categories_info_for_city(city: str) -> List[CategoryInfo]:
    categories = []
    categories_url = f"{BASE_UE_URL}/category/{parse_city(city)}"
    categories_res = requests.get(categories_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(categories_res.text, features="html.parser")
    matches = page_info.find_all(
        "a", href=lambda href: href and href.startswith("/category")
    )
    for match in tqdm(matches):
        try:
            categories.append(CategoryInfo(match["data-test"], match["href"]))
        except Exception:
            continue
    return categories


def get_item_info(item_url: str, browser: webdriver.Chrome) -> Optional[ItemInfo]:
    # full_url = "https://www.ubereats.com/store/homeroom-to-go/32drpQtyRNeTXI0jm6wP0A?diningMode=DELIVERY&mod=quickView&modctx=%257B%2522storeUuid%2522%253A%2522df676ba5-0b72-44d7-935c-8d239bac0fd0%2522%252C%2522sectionUuid%2522%253A%25227d980256-87b4-5f9d-980e-f5ceda776ec4%2522%252C%2522subsectionUuid%2522%253A%25227e12b8f3-15c3-520f-bd0a-58c7cee4ca68%2522%252C%2522itemUuid%2522%253A%2522386a3b04-283a-53f8-b6c2-4734846be037%2522%257D&ps=1"
    try:
        full_url = f"{BASE_UE_URL}{item_url}&diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE"
        browser.get(full_url)
        # element 0 is the restaurant name, element 1 is the item name
        item_element = browser.find_elements(By.TAG_NAME, "h1")[1]
        item_name = item_element.text
        item_description = None
        try:
            parent_element = item_element.find_element(By.XPATH, "..")
            child_elements = parent_element.find_elements(By.TAG_NAME, "div")
            for e in child_elements:
                if e.text:
                    item_description = e.text
                    break
            print(f"get_item_info {item_url}, {item_name}, {item_description}")
            return ItemInfo(item_name, item_description)
        except Exception as e:
            print(f"get_item_info {item_url}, {item_name}, {item_description}")
            # item_description will probably be None
            return ItemInfo(item_name, item_description)
    except Exception as e:
        return None


def get_restaurant_info(
    restaurant: RestaurantInfo,
    browser: webdriver.Chrome,
    executor: concurrent.futures.ThreadPoolExecutor,
    items_limit: Optional[int],
    sleep_sec: Optional[int],
) -> RestaurantInfo:
    restaurant_res = requests.get(
        f"{BASE_UE_URL}{restaurant.rel_url}", headers=BASE_HEADERS
    )
    menu_info = BeautifulSoup(restaurant_res.text, features="html.parser")
    item_url_matches = menu_info.find_all(
        "a", href=lambda href: href and href.startswith(restaurant.rel_url)
    )
    menu = {}
    futures_to_href = {}

    enumerated = 0
    for item_url_element in tqdm(item_url_matches):
        if items_limit is not None and enumerated == items_limit:
            break
        href = item_url_element["href"]
        if (
            href == restaurant.rel_url
            or "storeInfo" in href
            or "?mod=quickView" not in href
        ):
            continue
        # Sleep to avoid getting blocked
        time.sleep(sleep_sec or random.uniform(1, 5))
        futures_to_href[executor.submit(get_item_info, href, browser)] = href
        enumerated += 1

    for future in concurrent.futures.as_completed(futures_to_href):
        href = futures_to_href[future]
        try:
            item_info = future.result()
            menu[item_info.name] = item_info.description
        except Exception as exc:
            print(f"href {href} generated an exception: {exc}")

    if menu:
        return restaurant._replace(menu=menu)
    return restaurant


def get_restaurants(
    city: str,
    browser: webdriver.Chrome,
    executor: concurrent.futures.ThreadPoolExecutor,
    categories_limit: Optional[int] = None,
    restaurants_limit: Optional[int] = None,
    items_limit: Optional[int] = None,
    sleep_sec: Optional[int] = None,
) -> List[RestaurantInfo]:
    restaurants_csv_path = Path(f"restaurants-info-{parse_city(city)}.csv")
    categories_info = (
        get_categories_info_for_city(city)[:categories_limit]
        if categories_limit is not None
        else get_categories_info_for_city(city)
    )

    for category in tqdm(categories_info):
        restaurants = get_restaurants_in_category(
            category, restaurants_limit=restaurants_limit
        )
        for i in tqdm(range(len(restaurants))):
            restaurants[i] = get_restaurant_info(
                restaurants[i], browser, executor, items_limit, sleep_sec
            )
        # TODO: in the future have 1 row per menu item instead of 1 row per restaurant to avoid a huge menu dict
        with open(restaurants_csv_path, "a") as f:
            w = csv.writer(f)
            if not restaurants_csv_path.exists():
                w.writerow(["name", "rating", "url", "menu", "category"])
            w.writerows(
                [tuple(restaurant) + (category.name,) for restaurant in restaurants]
            )


def setup_browser() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Ensure GUI is off
    chrome_options.add_argument("--no-sandbox")
    homedir = os.path.expanduser("~")
    webdriver_service = Service(f"{homedir}/chromedriver/stable/chromedriver")
    browser = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    return browser


if __name__ == "__main__":
    print("doing something in a loop ...")
    # cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    cities = ["Emeryville"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for city in cities:
            get_restaurants(
                city,
                setup_browser(),
                executor,
                categories_limit=None,
                restaurants_limit=None,
                items_limit=10,
                sleep_sec=1,
            )
