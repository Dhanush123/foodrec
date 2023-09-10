# foodrec

Automated way to recommend and/or vote on delivery restaurant choices.

For context: at my job we have a Slack bot that daily asks employees which restaurant people want to order from (on UberEats) for dinner and people can pick multiple choices. I wanted to automate this for me. I did this by 1) scraping the menus (title + description of items) of UberEats restaurants in cities in/nearby the work office 2) scraping various food recipe websites for vegetarian recipes (since that's my diet) and 3) using cosine similarity between the two sets of data to score and rank the restaurants

flows/ contains Prefect flow definitions for 1) and 2) and can be run with main.py
restaurant_analytics.ipynb make use of TFIDF, KMeans, TSNE to analyze the similarity of restaurants
scoring.ipynb makes use of TFIDF, cosine similiarity, and various preprocessing methods to do 3)
alembic/ is for the SQLAlchemy ORM since all info from the flows is saved to a SQLite DB so it can be persisted between runs and used in the notebooks
utils/ contains various util methods and DB schemas

Tech stack: Prefect, SQLite, SQLAlchemy, requests, Ray (to parallelize Prefect tasks), scikit-learn, nltk, pandas, numpy, matplotlib, seaborn

TODOs:
- Scrape meat recipes too so the scoring can be much more accurate with a supervised machine learning approach
- Serve the predictions and integrate with Slack
