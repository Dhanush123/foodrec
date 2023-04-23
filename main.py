import fire
from flows.restaurant_stable import restaurants_flow
from flows.recipes_stable import recipes_flow


class Main(object):
    def __init__(self):
        pass

    def restaurants_flow(self):
        restaurants_flow()

    def recipes_flow(self):
        recipes_flow()


if __name__ == "__main__":
    fire.Fire(Main)
