from typing import List, NamedTuple

from sqlalchemy import (
    ForeignKey,
    create_engine,
    Column,
    Integer,
    String,
)
from sqlalchemy.orm import Session, declarative_base

DB_ENGINE = create_engine("sqlite:///data/menu.db", echo=False)


class CategoryInfo(NamedTuple):
    name: str
    rel_url: str


class RestaurantInfo(NamedTuple):
    name: str
    rating: float
    rel_url: str


class ItemInfo(NamedTuple):
    name: str
    description: str
    rel_url: str
    restaurant: RestaurantInfo


# This old syntax is due to using SQLAlchemy 1.4.22, latest 2.0.+ is not compatible with Prefect
# Gives error prefect 2.7.11 requires sqlalchemy[asyncio]!=1.4.33,<2.0,>=1.4.22, but you have sqlalchemy 2.0.4 which is incompatible
Base = declarative_base()


class Category(Base):
    __tablename__ = "category"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    rel_url = Column(String, unique=True)


class Restaurant(Base):
    __tablename__ = "restaurant"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    rating = Column(Integer)
    rel_url = Column(String, unique=True)
    category_id = Column(Integer, ForeignKey("category.id"))


class Item(Base):
    __tablename__ = "item"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    rel_url = Column(String, unique=True)
    restaurant_id = Column(Integer, ForeignKey("restaurant.id"))


class City(Base):
    __tablename__ = "city"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    rel_url = Column(String, unique=True)


def create_db_tables() -> None:
    Base.metadata.create_all(DB_ENGINE)


def save_categories_to_db(categories: List[CategoryInfo]) -> None:
    # combining the contexts means commit and close are implicitly called
    with Session(DB_ENGINE) as session, session.begin():
        for category in categories:
            session.add(Category(**category._asdict()))
        session.commit()
        session.close()


def save_restaurants_to_db(
    category: Category, restaurants: List[RestaurantInfo]
) -> None:
    # combining the contexts means commit and close are implicitly called
    with Session(DB_ENGINE) as session, session.begin():
        for restaurant in restaurants:
            session.add(Restaurant(category_id=category.id, **restaurant._asdict()))


def save_items_to_db(restaurant: Restaurant, items: List[ItemInfo]) -> None:
    with Session(DB_ENGINE) as session, session.begin():
        for item in items:
            session.add(Item(restaurant_id=restaurant.id, **item._asdict()))
