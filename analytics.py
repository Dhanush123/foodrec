# from datetime import datetime, timedelta, timezone
# import pandas as pd
# from utils import get_db_con


# if __name__ == "__main__":
#     with get_db_con() as db_con:
#         query = "SELECT * FROM items"
#         # cur = db_con.execute("SELECT * FROM items")
#         # results = cur.fetchall()
#         db_df = pd.read_sql_query(query, db_con)
#         # UTC is 8 hours ahead of PT
#         pt_now = datetime.now(timezone.utc) - timedelta(hours=8)
#         db_df.to_csv(f"items_{pt_now.isoformat()}.csv")
