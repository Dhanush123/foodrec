from pathlib import Path
import random
import time
from typing import Callable, List, Optional
from bs4 import BeautifulSoup
from prefect import flow, task, unmapped
from prefect_ray.task_runners import RayTaskRunner
from prefect_ray.context import remote_options

import requests

from utils.utils import DEFAULT_SLEEP_SEC, TASK_TIMEOUT_SECONDS
from utils.db_utils import RecipeInfo, create_db_tables, save_recipe_to_db


def _get_allrecipes_urls(offset: int) -> List[str]:
    # increment by 24 from offset
    return f"https://www.allrecipes.com/search?vegetarian=vegetarian&offset={offset+24}&q=vegetarian"


@task()
def get_allrecipes_urls(num_recipes_limit: int) -> List[str]:
    all_recipe_urls = []
    try:
        offset = -24
        while len(all_recipe_urls) < num_recipes_limit:
            try:
                url = _get_allrecipes_urls(offset=offset)
                response = requests.get(url)
                soup = BeautifulSoup(response.text, "html.parser")

                cur_page_recipe_urls = []
                main_element = soup.find("main")
                if main_element:
                    for a_element in main_element.find_all("a", href=True):
                        href = a_element["href"]
                        if href.startswith("https://www.allrecipes.com/recipes/"):
                            cur_page_recipe_urls.append(href)

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


def process_direct_recipe_url(
    recipe_url: str, sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC
) -> Optional[RecipeInfo]:
    time.sleep(random.uniform(0, sleep_sec))
    try:
        response = requests.get(recipe_url)
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
        return RecipeInfo(name=name, ingredients=ingredients_str, url=recipe_url)
    except Exception as e:
        print(f"Could not process recipe url {recipe_url}: {e}")
        return None


def get_recipe_urls_from_collection_url(recipe_url: str) -> List[str]:
    try:
        response = requests.get(recipe_url)
        soup = BeautifulSoup(response.text, "html.parser")
        recipe_urls = []
        main_element = soup.find("main")
        if main_element:
            for a_element in main_element.find_all("a", href=True):
                href = a_element["href"]
                if href.startswith("https://www.allrecipes.com/recipe/"):
                    recipe_urls.append(href)
        return recipe_urls
    except Exception as e:
        print(f"Could not get recipe urls from collection url {recipe_url}: {e}")
        return []


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


@task(on_completion=[save_recipe_to_db_hook])
def process_recipe_url(
    recipe_url: str,
    sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC,
) -> Optional[List[RecipeInfo]]:
    try:
        recipe_info = process_direct_recipe_url(recipe_url, sleep_sec)
        if recipe_info:
            return [recipe_info]
        else:
            recipe_infos = []
            # Must be a page that list recipes, so get the direct recipe urls and process them
            recipe_urls = get_recipe_urls_from_collection_url(recipe_url)
            for recipe_url in recipe_urls:
                recipe_info = process_direct_recipe_url(recipe_url)
                if recipe_info:
                    recipe_infos.append(recipe_info)
            return recipe_infos
    except Exception as e:
        print(f"Could not process recipe url {recipe_url}: {e}")
        return None


@flow(task_runner=RayTaskRunner())
def recipes_flow():
    num_cpus = 12
    sleep_sec = num_cpus * 0.1
    num_recipes_limit = 1

    # 1
    create_db_tables()

    # 2
    allrecipes_urls = get_allrecipes_urls(num_recipes_limit)

    with remote_options(num_cpus=num_cpus):
        # 3
        for recipe_url in allrecipes_urls:
            process_recipe_url.submit(recipe_url, sleep_sec)
