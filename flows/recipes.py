import random
import time
from typing import List
from bs4 import BeautifulSoup
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner
from prefect_ray.context import remote_options
import requests

from utils.db_utils import create_db_tables, save_recipe_to_db


@task()
def get_allrecipes_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.select(".fixed-recipe-card__h3 a")]
    return links


@task()
def get_allrecipes_details(url: str, sleep_sec: float) -> dict:
    time.sleep(random.uniform(0, sleep_sec))

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    name = soup.select_one(".headline-wrapper h1").text.strip()
    ingredients = [i.text.strip() for i in soup.select(".ingredients-item-name")]

    return {"name": name, "ingredients": ingredients}


@task()
def get_food_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.select(".tile__title-link")]
    return links


@task()
def get_food_details(url: str, sleep_sec: float) -> dict:
    time.sleep(random.uniform(0, sleep_sec))

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    name = soup.select_one(".recipe-title").text.strip()
    ingredients = [
        i.text.strip()
        for i in soup.select(".recipe-ingredients__ingredient-parts .ingredient")
    ]

    return {"name": name, "ingredients": ingredients}


@task
def get_foodnetwork_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.select(".card.recipe-card .card__content a")]
    return links


@task
def get_foodnetwork_details(url: str, sleep_sec: float) -> dict:
    time.sleep(random.uniform(0, sleep_sec))

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    name = soup.select_one(".o-AssetTitle__a-HeadlineText").text.strip()
    ingredients = [
        i.text.strip() for i in soup.select(".o-Ingredients__a-ListItemText")
    ]

    return {"name": name, "ingredients": ingredients}


@task
def get_delish_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.select(".full-item-content a")]
    return links


@task
def get_delish_details(url: str, sleep_sec: float) -> dict:
    time.sleep(random.uniform(0, sleep_sec))

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    name = soup.select_one(".recipe-title").text.strip()
    ingredients = [i.text.strip() for i in soup.select(".ingredient-description")]

    return {"name": name, "ingredients": ingredients}


@task
def get_nytimes_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.select(".css-4jyr1y a")]
    return links


@task
def get_nytimes_details(url: str, sleep_sec: float) -> dict:
    time.sleep(random.uniform(0, sleep_sec))

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    name = soup.select_one(".css-jeyium").text.strip()
    ingredients = [i.text.strip() for i in soup.select(".css-8z9nqv")]

    return {"name": name, "ingredients": ingredients}


@task
def get_bonappetit_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.select(".card-hed a")]
    return links


@task
def get_bonappetit_details(url: str, sleep_sec: float) -> dict:
    time.sleep(random.uniform(0, sleep_sec))

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    name = soup.select_one(".post__header__hed").text.strip()
    ingredients = [i.text.strip() for i in soup.select(".ingredients__text")]

    return {"name": name, "ingredients": ingredients}


@task
def get_urls(page_num):
    allrecipes_base_url = (
        "https://www.allrecipes.com/recipes/87/everyday-cooking/vegetarian/"
    )
    food_base_url = "https://www.food.com/recipe/all/browse?tags=vegetarian"
    foodnetwork_base_url = "https://www.foodnetwork.com/recipes/recipes-a-z/vegetarian-"
    delish_base_url = "https://www.delish.com/vegetarian-recipes/"
    nytimes_base_url = "https://cooking.nytimes.com/recipes/browse/vegetarian?page="
    bonappetit_base_url = "https://www.bonappetit.com/recipes/vegetarian?page="

    allrecipes_urls = [
        f"{allrecipes_base_url}?page={page}" for page in range(1, page_num + 1)
    ]
    food_urls = [f"{food_base_url}&pn={page}" for page in range(1, page_num + 1)]
    foodnetwork_urls = [
        f"{foodnetwork_base_url}{page}" for page in range(1, page_num + 1)
    ]
    delish_urls = [f"{delish_base_url}p/{page}" for page in range(1, page_num + 1)]
    nytimes_urls = [f"{nytimes_base_url}{page}" for page in range(1, page_num + 1)]
    bonappetit_urls = [
        f"{bonappetit_base_url}{page}" for page in range(1, page_num + 1)
    ]

    return (
        allrecipes_urls,
        food_urls,
        foodnetwork_urls,
        delish_urls,
        nytimes_urls,
        bonappetit_urls,
    )


@task
def process_website_links(
    website_links: List[str], website_details_task, sleep_sec: float
):
    for link in website_links:
        recipe_details = website_details_task.submit(link, sleep_sec)
        save_recipe_to_db.submit(recipe_details)


@flow(task_runner=RayTaskRunner())
def recipes_flow():
    num_cpus = 20
    num_pages = 1
    sleep_sec = num_cpus * 0.5

    # 1
    create_db_tables()

    # 2
    (
        allrecipes_urls,
        food_urls,
        foodnetwork_urls,
        delish_urls,
        nytimes_urls,
        bonappetit_urls,
    ) = get_urls(num_pages)

    # 3
    with remote_options(num_cpus=num_cpus):
        tasks = [
            (get_allrecipes_links, get_allrecipes_details, allrecipes_urls),
            (get_food_links, get_food_details, food_urls),
            (get_foodnetwork_links, get_foodnetwork_details, foodnetwork_urls),
            (get_delish_links, get_delish_details, delish_urls),
            (get_nytimes_links, get_nytimes_details, nytimes_urls),
            (get_bonappetit_links, get_bonappetit_details, bonappetit_urls),
        ]

        for get_links_task, get_details_task, urls in tasks:
            for url in urls:
                links = get_links_task.submit(url)
                process_website_links.submit(links, get_details_task, sleep_sec)
