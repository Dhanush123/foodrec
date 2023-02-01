
from utils import get_db_con


if __name__ == "__main__":
    with get_db_con() as db_con:
        cur = db_con.execute("SELECT * FROM items")
        print(cur.fetchall())
