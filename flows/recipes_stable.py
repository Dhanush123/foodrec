from itertools import chain
import json
import random
import time
from typing import List, NamedTuple, Optional
from tqdm import tqdm
from bs4 import BeautifulSoup
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner
from prefect_ray.context import remote_options

import requests

from utils.utils import (
    DEFAULT_SLEEP_SEC,
    REQUEST_GET_TIMEOUT_SECS,
    TASK_TIMEOUT_SECONDS,
)
from utils.db_utils import RecipeInfo, create_db_tables, save_recipe_to_db

TASK_TIMEOUT_30_MIN = 1800


# UrlType Enum
class UrlType:
    ALLRECIPES = "allrecipes"
    NYT_COOKING = "nytcooking"


# NamedTuple with url and type
class RecipeUrl(NamedTuple):
    url: str
    type: UrlType


def _get_allrecipes_urls(offset: int) -> List[str]:
    # increment by 24 from offset
    return f"https://www.allrecipes.com/search?vegetarian=vegetarian&offset={offset+24}&q=vegetarian"


@task(timeout_seconds=TASK_TIMEOUT_30_MIN)
def get_allrecipes_urls(num_recipes_limit: int) -> List[RecipeUrl]:
    # 30 min timeout, unlikely
    all_recipe_urls = []
    try:
        offset = -24
        while len(all_recipe_urls) < num_recipes_limit:
            try:
                url = _get_allrecipes_urls(offset=offset)
                response = requests.get(url, timeout=REQUEST_GET_TIMEOUT_SECS)
                soup = BeautifulSoup(response.text, "html.parser")

                cur_page_recipe_urls = []
                main_element = soup.find("main")
                if main_element:
                    for a_element in tqdm(main_element.find_all("a", href=True)):
                        href = a_element["href"]
                        if href.startswith("https://www.allrecipes.com/recipes/"):
                            cur_page_recipe_urls.append(
                                RecipeUrl(url=href, type=UrlType.ALLRECIPES)
                            )

                if not cur_page_recipe_urls:
                    print(f"Could not find any recipe urls at offset {offset}")
                    break
                all_recipe_urls.extend(cur_page_recipe_urls)
                offset += 24
                print(
                    f"Found {len(all_recipe_urls)} recipe urls so far at offset {offset}"
                )
            except Exception as e:
                print(f"Could not get allrecipes urls with offset {offset}: {e}")
                break
        return all_recipe_urls
    except Exception as e:
        print(f"Could not get allrecipes urls: {e}")
        return all_recipe_urls


def _get_nytcooking_urls(page_num: int) -> List[str]:
    return f"https://cooking.nytimes.com/search?q=vegetarian&tags=vegetarian&page={page_num}"


@task(timeout_seconds=TASK_TIMEOUT_30_MIN)
def get_nytcooking_urls(num_recipes_limit: int) -> List[RecipeUrl]:
    all_recipe_urls = []
    try:
        page_num = 1
        while len(all_recipe_urls) < num_recipes_limit:
            try:
                url = _get_nytcooking_urls(page_num=page_num)
                response = requests.get(url, timeout=REQUEST_GET_TIMEOUT_SECS)
                soup = BeautifulSoup(response.text, "html.parser")

                cur_page_recipe_urls = []
                for a_element in tqdm(soup.find_all("a", href=True)):
                    href = a_element["href"]
                    if href.startswith("/recipes/"):
                        cur_page_recipe_urls.append(
                            RecipeUrl(
                                url=f"https://cooking.nytimes.com{href}",
                                type=UrlType.NYT_COOKING,
                            )
                        )

                all_recipe_urls.extend(cur_page_recipe_urls)

                print(
                    f"Found {len(all_recipe_urls)} recipe urls so far at page {page_num}"
                )
                page_num += 1
            except Exception as e:
                print(f"Could not get nyt cooking urls with page {page_num}: {e}")
                break
        return all_recipe_urls
    except Exception as e:
        print(f"Could not get nyt cooking urls: {e}")
        return all_recipe_urls


def process_direct_recipe_url_allrecipes(recipe_url: RecipeUrl) -> Optional[RecipeInfo]:
    response = requests.get(recipe_url.url, timeout=REQUEST_GET_TIMEOUT_SECS)
    soup = BeautifulSoup(response.text, "html.parser")
    h1_element = soup.find("h1")
    name = h1_element.text.strip() if h1_element else None
    if not name:
        print(f"Could not find name for recipe url {recipe_url}")
        return None

    ingredient_spans = soup.find_all("span", {"data-ingredient-name": "true"})
    # DB stores ingredients a string so the list must be converted
    ingredients_list = [span.text.strip() for span in ingredient_spans]
    if not ingredients_list:
        print(f"Could not find ingredients for recipe url {recipe_url}")
        return None

    ingredients_str = ",".join(ingredients_list)
    print(f"Found recipe {name} with {ingredients_str} and {recipe_url}")
    return RecipeInfo(name=name, ingredients=ingredients_str, url=recipe_url.url)


def process_direct_recipe_url_nytcooking(recipe_url: RecipeUrl) -> Optional[RecipeInfo]:
    requests.packages.urllib3.disable_warnings()

    # Get the page source
    page_source = requests.get(
        recipe_url.url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=REQUEST_GET_TIMEOUT_SECS,
    ).content

    # Parse the page source with BeautifulSoup
    soup = BeautifulSoup(page_source, "lxml")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    script_content = script.contents[0]
    script_dict = json.loads(script_content)
    recipe_ingredients = script_dict["props"]["pageProps"]["recipe"]["ingredients"][0][
        "ingredients"
    ]
    ingredients_list = [ingredient["text"] for ingredient in recipe_ingredients]
    if ingredients_list:
        ingredients_str = ",".join(ingredients_list)
        print(f"Found recipe {recipe_url} with {ingredients_str}")
        return RecipeInfo(name=None, ingredients=ingredients_str, url=recipe_url.url)
    else:
        print(f"Could not find ingredients for recipe url {recipe_url}")
        return None


def process_direct_recipe_url(
    recipe_url: RecipeUrl, sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC
) -> Optional[RecipeInfo]:
    time.sleep(random.uniform(0, sleep_sec))
    try:
        if recipe_url.type == UrlType.ALLRECIPES:
            return process_direct_recipe_url_allrecipes(recipe_url=recipe_url)
        elif recipe_url.type == UrlType.NYT_COOKING:
            return process_direct_recipe_url_nytcooking(recipe_url=recipe_url)
    except Exception as e:
        print(f"Could not process recipe url {recipe_url}: {e}")
        return None


def process_collection_recipe_url_allrecipes(
    recipe_url: RecipeUrl,
) -> Optional[List[str]]:
    response = requests.get(recipe_url.url, timeout=REQUEST_GET_TIMEOUT_SECS)
    soup = BeautifulSoup(response.text, "html.parser")
    recipe_urls = []
    main_element = soup.find("main")
    if main_element:
        for a_element in main_element.find_all("a", href=True):
            href = a_element["href"]
            if href.startswith("https://www.allrecipes.com/recipe/"):
                recipe_urls.append(href)
        return recipe_urls
    return None


def get_recipe_urls_from_collection_url(
    recipe_url: RecipeUrl,
) -> Optional[List[str]]:
    try:
        if recipe_url.type == UrlType.ALLRECIPES:
            return process_collection_recipe_url_allrecipes(recipe_url=recipe_url)
        elif recipe_url.type == UrlType.NYT_COOKING:
            return None
    except Exception as e:
        print(f"Could not get recipe urls from collection url {recipe_url}: {e}")
        return None


def save_recipe_to_db_hook(task, task_run, state) -> None:
    recipes = state.result()
    if not recipes:
        return
    for recipe in recipes:
        if recipe:
            try:
                save_recipe_to_db(recipe)
            except Exception as e:
                print(
                    f"Could not save recipe {recipe} to DB. Maybe it already exists in the DB: {e}"
                )


@task(on_completion=[save_recipe_to_db_hook], timeout_seconds=TASK_TIMEOUT_30_MIN)
def process_recipe_url(
    original_recipe_url: RecipeUrl,
    sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC,
) -> Optional[List[RecipeInfo]]:
    try:
        # All NYT cooking recipes, but only some AllRecipe, recipes are direct recipes
        recipe_info = process_direct_recipe_url(original_recipe_url, sleep_sec)
        if recipe_info:
            return [recipe_info]
        else:
            recipe_infos = []
            # Must be a page that list recipes, so get the direct recipe urls and process them
            actual_recipe_urls: Optional[
                List[str]
            ] = get_recipe_urls_from_collection_url(original_recipe_url)
            if not actual_recipe_urls:
                return None
            for actual_recipe_url in actual_recipe_urls:
                recipe_info = process_direct_recipe_url(
                    RecipeUrl(url=actual_recipe_url, type=original_recipe_url.type),
                    sleep_sec,
                )
                if recipe_info:
                    recipe_infos.append(recipe_info)
            return recipe_infos
    except Exception as e:
        print(f"Could not process recipe url {original_recipe_url}: {e}")
        return None


@flow(task_runner=RayTaskRunner())
def recipes_flow():
    num_cpus = 10
    sleep_sec = num_cpus * 0.2
    num_recipes_limit = 100

    # 1
    create_db_tables()

    # 2
    allrecipes_urls = get_allrecipes_urls(num_recipes_limit)
    nytcooking_urls = get_nytcooking_urls(num_recipes_limit, wait_for=[allrecipes_urls])
    # allrecipes_urls = []
    # nytcooking_urls = get_nytcooking_urls(num_recipes_limit)

    with remote_options(num_cpus=num_cpus):
        # 3
        for recipe_url in chain(allrecipes_urls, nytcooking_urls):
            process_recipe_url.submit(recipe_url, sleep_sec)
