import random
import time
from typing import List, Optional
import concurrent.futures

from bs4 import BeautifulSoup
import requests


from utils import (
    BASE_HEADERS,
    BASE_UE_URL,
    CategoryInfo,
    ItemInfo,
    RestaurantInfo,
    add_items_to_db,
    create_db,
    get_db_con,
    parse_city,
)


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

    restaurant_list = []
    enumerated = 0
    for header in page_info.find_all("h3"):
        if restaurants_limit is not None and enumerated == restaurants_limit:
            break
        try:
            if header.parent is None or header.parent.get("href") is None:
                continue
            rel_restaurant_url = header.parent.get("href")
            # Some urls might not be a restaurant
            if rel_restaurant_url.startswith("/store"):
                restaurant_name = header.get_text()
                rating = get_rating_from_restaurant_box(header.parent.parent)
                restaurant_list.append(
                    RestaurantInfo(
                        restaurant_name, rating, rel_restaurant_url, category
                    )
                )
                enumerated += 1
        except Exception as e:
            print(f"While getting restaurant from match: {header}, got exception: {e}")
            continue
    print(f"Found {len(restaurant_list)} restaurants in {category.name}")
    return restaurant_list


def get_categories_info_for_city(city: str) -> List[CategoryInfo]:
    categories = []
    categories_url = f"{BASE_UE_URL}/category/{parse_city(city)}"
    categories_res = requests.get(categories_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(categories_res.text, features="html.parser")
    matches = page_info.find_all(
        "a", href=lambda href: href and href.startswith("/category")
    )
    for match in matches:
        try:
            categories.append(CategoryInfo(match["data-test"], match["href"]))
        except Exception:
            continue
    return categories


def get_menu_items(
    restaurant: RestaurantInfo,
    items_limit: Optional[int],
) -> List[ItemInfo]:
    restaurant_res = requests.get(
        f"{BASE_UE_URL}{restaurant.rel_url}", headers=BASE_HEADERS
    )
    menu_info = BeautifulSoup(restaurant_res.text, features="html.parser")
    # There seem to be duplicate items/hrefs
    item_hrefs = set(
        item_url_element["href"]
        for item_url_element in menu_info.find_all(
            "a", href=lambda href: href and href.startswith(restaurant.rel_url)
        )
    )

    # Ensure valid data
    items_hrefs_to_remove = set()
    for href in item_hrefs:
        if (
            href == restaurant.rel_url
            or "storeInfo" in href
            or "?mod=quickView" not in href
        ):
            items_hrefs_to_remove.add(href)
    item_hrefs -= items_hrefs_to_remove

    item_hrefs = list(item_hrefs)
    final_items_limit = items_limit or len(item_hrefs)
    items = [
        ItemInfo(name=None, description=None, rel_url=url, restaurant=restaurant)
        for url in item_hrefs[:final_items_limit]
    ]
    print(f"Found {len(items)} items in {restaurant.name}")
    return items


def get_categories_in_city(
    city: str,
    categories_limit: Optional[int] = None,
) -> List[CategoryInfo]:

    categories = []
    categories_url = f"{BASE_UE_URL}/category/{parse_city(city)}"
    categories_res = requests.get(categories_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(categories_res.text, features="html.parser")
    matches = page_info.find("main").find_all(
        "a", href=lambda href: href and href.startswith("/category")
    )
    final_categories_limit = categories_limit or len(matches)
    for i, match in enumerate(matches):
        if i == final_categories_limit:
            break
        try:
            categories.append(CategoryInfo(match.get("data-test"), match.get("href")))
        except Exception as e:
            print(f"While getting category from match: {match}, got exception: {e}")
            continue
    print(
        f"Returning {final_categories_limit} categories out of {len(matches)} matches for {city}."
    )
    return categories


if __name__ == "__main__":
    cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    num_threads = 12
    categories_limit, restaurants_limit, items_limit = None, None, None

    city_futures_to_categories = {}
    category_futures_to_restaurants = {}
    item_futures_to_db = {}

    with get_db_con() as db_con:
        create_db(db_con)

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for city in cities:
                city_future = executor.submit(
                    get_categories_in_city, city, categories_limit
                )
                city_futures_to_categories[city_future] = city
                time.sleep(random.uniform(1, 3))

            for city_future in concurrent.futures.as_completed(
                city_futures_to_categories
            ):
                categories: List[CategoryInfo] = city_future.result()
                for category in categories:
                    category_future = executor.submit(
                        get_restaurants_in_category, category, restaurants_limit
                    )
                    category_futures_to_restaurants[category_future] = category
                    time.sleep(random.uniform(1, 3))

            for category_future in concurrent.futures.as_completed(
                category_futures_to_restaurants
            ):
                restaurants: List[RestaurantInfo] = category_future.result()
                for restaurant in restaurants:
                    item_future = executor.submit(
                        get_menu_items, restaurant, items_limit
                    )
                    item_futures_to_db[item_future] = item_future
                    time.sleep(random.uniform(1, 3))

            for item_future in concurrent.futures.as_completed(item_futures_to_db):
                items: List[ItemInfo] = item_future.result()
                print(
                    f"Inserting {len(items)} items into the DB for {items[0].restaurant.name}."
                )
                add_items_to_db(db_con, items)

    db_con.close()
