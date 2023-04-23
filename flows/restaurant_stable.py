import json
import random
import time
from typing import List, Optional

from bs4 import BeautifulSoup
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner
from prefect_ray.context import remote_options
import requests


from utils.utils import (
    DEFAULT_SLEEP_SEC,
    TASK_TIMEOUT_SECONDS,
    BASE_HEADERS,
    BASE_UE_URL,
    parse_city,
)
from utils.db_utils import (
    Category,
    CategoryInfo,
    Item,
    ItemInfo,
    Restaurant,
    RestaurantInfo,
    create_db_tables,
    get_categories_from_db,
    get_items_from_db,
    get_restaurants_from_db,
    save_categories_to_db,
    save_restaurants_to_db,
    save_items_to_db,
)


def _get_rating_from_restaurant_box(restaurant_box: BeautifulSoup) -> int:
    for child in restaurant_box.findChildren("div", recursive=True):
        try:
            rating = float(child.text)
            return rating
        except Exception:
            continue
    return 0


@task
def get_items_in_restaurant(
    restaurant: Restaurant,
    sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC,
) -> List[ItemInfo]:
    time.sleep(random.uniform(0, sleep_sec))
    # full_url = "https://www.ubereats.com/store/la-estrella-food-truck/1S1RJ9zXQC23uwBxwtXR3A?diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE"
    full_url = f"{BASE_UE_URL}{restaurant.rel_url}?diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE"
    print(f"Getting items from restaurant: {restaurant.name} with url: {full_url}")
    res = requests.get(full_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(res.text, features="html.parser")
    matches = page_info.find_all("script", type="application/ld+json")
    all_item_infos = []
    try:
        for match in matches:
            match = json.loads(match.text)
            if match.get("@type") == "Restaurant":
                menu = match.get("hasMenu")
                if menu:
                    menu_selection = menu.get("hasMenuSection")
                    if menu_selection:
                        for menu in menu_selection:
                            menu_items = menu.get("hasMenuItem")
                            if menu_items:
                                for item in menu_items:
                                    name, description = item.get("name"), item.get(
                                        "description"
                                    )
                                    dummy_rel_url = f"{name}+{restaurant.id}"
                                    # TODO: drop rel_url col fro DB and info
                                    item_info = ItemInfo(
                                        name, description, dummy_rel_url
                                    )
                                    all_item_infos.append(item_info)
                break
        print(
            f"Saving {len(all_item_infos)} items for restaurant: {restaurant.name} to DB"
        )
        save_items_to_db(restaurant, all_item_infos)
    except Exception as e:
        print(
            f"While getting items from restaurant: {restaurant.name}, got exception: {e}"
        )
    finally:
        return all_item_infos


@task(timeout_seconds=TASK_TIMEOUT_SECONDS)
def get_restaurants_in_category(
    category: Category,
    restaurants_limit: Optional[int],
    sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC,
) -> List[RestaurantInfo]:
    time.sleep(random.uniform(0, sleep_sec))
    restaurants = []

    try:
        # TODO: filter out restaurants that are too far for delivery
        category_res = requests.get(
            f"{BASE_UE_URL}{category.rel_url}", headers=BASE_HEADERS
        )
        page_info = BeautifulSoup(category_res.text, features="html.parser")

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
                    rating = _get_rating_from_restaurant_box(header.parent.parent)
                    restaurants.append(
                        RestaurantInfo(restaurant_name, rating, rel_restaurant_url)
                    )
                    enumerated += 1
            except Exception as e:
                print(
                    f"While getting restaurant from match: {header}, got exception: {e}"
                )
                continue

        print(f"Found {len(restaurants)} restaurants in {category.name}")
        save_restaurants_to_db(category, restaurants)
    except Exception as e:
        print(
            f"While getting restaurants in category: {category.name}, got exception: {e}"
        )
    finally:
        return restaurants


@task(timeout_seconds=TASK_TIMEOUT_SECONDS)
def get_categories_in_city(
    city: str,
    categories_limit: Optional[int] = None,
    sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC,
) -> List[CategoryInfo]:
    time.sleep(random.uniform(0, sleep_sec))
    categories = []

    try:
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
                name, rel_url = match.get("data-test"), match.get("href")
                categories.append(CategoryInfo(name, rel_url))
            except Exception as e:
                print(f"While getting category from match: {match}, got exception: {e}")
                continue

        print(
            f"Saving {final_categories_limit} categories out of {len(matches)} matches for {city}."
        )
        save_categories_to_db(categories)
    except Exception as e:
        print(f"While getting categories for city: {city}, got exception: {e}")
    finally:
        return categories


@task(timeout_seconds=TASK_TIMEOUT_SECONDS)
def get_categories_from_db_task() -> List[Category]:
    return get_categories_from_db()


@task(timeout_seconds=TASK_TIMEOUT_SECONDS)
def get_restaurants_from_db_task() -> List[Restaurant]:
    return get_restaurants_from_db()


@task(timeout_seconds=TASK_TIMEOUT_SECONDS)
def get_items_from_db_task() -> List[Item]:
    return get_items_from_db()


@flow(task_runner=RayTaskRunner())
def restaurants_flow():
    # cities = ["Emeryville", "Oakland"]
    # categories_limit, restaurants_limit, items_limit = 2, 2, 2
    cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    categories_limit, restaurants_limit = None, None
    num_cpus = 20
    sleep_sec = num_cpus * 0.5

    print(
        f"Starting the flow with cities {cities}, {categories_limit} categories, and {restaurants_limit} restaurants per restaurant for the DB."
    )

    # 1
    create_db_tables()

    with remote_options(num_cpus=num_cpus):
        # 2
        category_futures = [
            get_categories_in_city.submit(city, categories_limit, sleep_sec)
            for city in cities
        ]
        # 3
        categories = get_categories_from_db_task(wait_for=category_futures)
        restaurant_futures = [
            get_restaurants_in_category.submit(category, restaurants_limit, sleep_sec)
            for category in categories
        ]
        # 4
        restaurants = get_restaurants_from_db_task(wait_for=restaurant_futures)
        for restaurant in restaurants:
            get_items_in_restaurant.submit(restaurant, sleep_sec)
