import random
import time
from typing import List, Optional
import concurrent.futures

from bs4 import BeautifulSoup
from prefect import flow, task, unmapped
from prefect.task_runners import ConcurrentTaskRunner

# from prefect_ray.task_runners import RayTaskRunner
# from prefect_ray.context import remote_options

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


# @task(timeout_seconds=10)
@task
def get_restaurants_in_category(
    categories: List[CategoryInfo], restaurants_limit: Optional[int]
) -> List[RestaurantInfo]:
    merged_restaurants = []

    for category in categories:
        try:
            time.sleep(random.uniform(0, 4))

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
                    print(
                        f"While getting restaurant from match: {header}, got exception: {e}"
                    )
                    continue
            print(f"Found {len(restaurant_list)} restaurants in {category.name}")
            merged_restaurants.extend(restaurant_list)
        except Exception as e:
            print(
                f"While getting restaurants in category: {category}, got exception: {e}"
            )
            continue

    return merged_restaurants


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


# @task(timeout_seconds=10)
@task
def get_menu_items(
    restaurants: List[RestaurantInfo],
    items_limit: Optional[int],
) -> List[ItemInfo]:
    merged_items = []

    for restaurant in restaurants:
        try:
            time.sleep(random.uniform(0, 4))

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
                ItemInfo(
                    name=None, description=None, rel_url=url, restaurant=restaurant
                )
                for url in item_hrefs[:final_items_limit]
            ]
            print(f"Found {len(items)} items in {restaurant.name}")
            merged_items.extend(items)
        except Exception as e:
            print(
                f"While getting items in restaurant: {restaurant}, got exception: {e}"
            )
            continue

    return merged_items


# @task(timeout_seconds=10)
@task
def get_categories_in_city(
    city: str,
    categories_limit: Optional[int] = None,
) -> List[CategoryInfo]:
    time.sleep(random.uniform(0, 4))
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
                categories.append(
                    CategoryInfo(match.get("data-test"), match.get("href"))
                )
            except Exception as e:
                print(f"While getting category from match: {match}, got exception: {e}")
                continue
        print(
            f"Returning {final_categories_limit} categories out of {len(matches)} matches for {city}."
        )
    except Exception as e:
        print(f"While getting categories for city: {city}, got exception: {e}")
    finally:
        return categories


@task(timeout_seconds=300)
def save_item_to_db(items: List[ItemInfo], db_timeout_sec: int):
    try:
        time.sleep(random.uniform(0, 4))
        with get_db_con(db_timeout_sec) as db_con:
            add_items_to_db(db_con, items)
        db_con.close()
    except Exception as e:
        print(f"While saving items to db, got exception: {e}")


@flow(task_runner=ConcurrentTaskRunner())
def restaurants_flow():
    # cities = ["Emeryville", "Oakland"]
    cities = ["Emeryville", "Oakland"]
    # cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    categories_limit, restaurants_limit, items_limit = None, 5, 50
    num_cpus = 16
    db_timeout_sec = 300

    with get_db_con() as db_con:
        create_db(db_con)
    db_con.close()

    # with remote_options(num_cpus=num_cpus):
    #     for city in cities:
    #         categories = get_categories_in_city.submit(city, categories_limit)
    #         for category in categories:
    #             restaurants = get_restaurants_in_category.submit(
    #                 category, restaurants_limit
    #             )
    #             if not restaurant_futures.get_state().is_failed():
    #                 for restaurant in restaurants:
    #                     items: List[ItemInfo] = get_menu_items.submit(
    #                         restaurant, items_limit
    #                     ).result(raise_on_failure=False)
    #                     save_items_to_db.submit(items).result(
    #                         raise_on_failure=False
    #                     )

    ############################################################################################

    # with remote_options(num_cpus=num_cpus):
    category_futures = get_categories_in_city.map(cities, categories_limit)
    restaurant_futures = get_restaurants_in_category.map(
        category_futures, restaurants_limit
    )
    item_futures = get_menu_items.map(restaurant_futures, items_limit)
    save_item_to_db.map(item_futures, db_timeout_sec)

    ############################################################################################

    # for city in cities:
    #     category_futures = get_categories_in_city.submit(city, categories_limit)
    #     if not category_futures.get_state().is_failed():
    #         categories: List[CategoryInfo] = category_futures.result(
    #             raise_on_failure=False
    #         )
    #         for category in categories:
    #             restaurant_futures = get_restaurants_in_category.submit(
    #                 category, restaurants_limit
    #             )
    #             restaurants: List[RestaurantInfo] = restaurant_futures.result(
    #                 raise_on_failure=False
    #             )
    #             if not restaurant_futures.get_state().is_failed():
    #                 for restaurant in restaurants:
    #                     items: List[ItemInfo] = get_menu_items.submit(
    #                         restaurant, items_limit
    #                     ).result(raise_on_failure=False)
    #                     save_items_to_db.submit(items).result(
    #                         raise_on_failure=False
    #                     )

    ############################################################################################

    # category_futures = get_categories_in_city.map(cities, categories_limit)
    # flat_category_infos = []
    # for future in category_futures:
    #     try:
    #         category_infos = future.result()
    #         if category_infos is not None:
    #             flat_category_infos.extend(category_infos)
    #         else:
    #             print(
    #                 f"Unexpected: category_infos future {future} result {category_infos}"
    #             )
    #     except Exception as e:
    #         print(f"Exception category_futures: {e}")
    #         continue

    # restaurant_futures = restaurant_futures = get_restaurants_in_category.map(
    #     flat_category_infos, restaurants_limit
    # )
    # flat_restaurant_infos = []
    # for future in restaurant_futures:
    #     try:
    #         restaurant_infos = future.result()
    #         if restaurant_infos is not None:
    #             flat_restaurant_infos.extend(restaurant_infos)
    #         else:
    #             print(
    #                 f"Unexpected: restaurant_infos future {future} was result {restaurant_infos}"
    #             )
    #     except Exception as e:
    #         print(f"Exception restaurant_futures: {e}")
    #         continue

    # item_futures = get_menu_items.map(flat_restaurant_infos, items_limit)
    # flat_item_infos = []
    # for future in item_futures:
    #     try:
    #         item_infos = future.result()
    #         if item_infos is not None:
    #             flat_item_infos.extend(item_infos)
    #         else:
    #             print(f"Unexpected: item_infos future {future} was result {item_infos}")
    #     except Exception as e:
    #         print(f"Exception item_futures: {e}")
    #         continue

    # save_item_to_db.map(flat_item_infos)


if __name__ == "__main__":
    restaurants_flow()
