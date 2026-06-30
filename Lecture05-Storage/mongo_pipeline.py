import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import random

# ==========================
# CONNECT MONGODB
# ==========================

client = MongoClient(
    "mongodb://admin:secret@localhost:27017/"
)

db = client["socialmedia_db"]

# Xóa dữ liệu cũ
db.users.drop()
db.posts.drop()
db.feeds.drop()

print("Database cleaned.")

# ==========================
# LOAD USERS
# ==========================

print("Loading users...")

users_df = pd.read_csv("SocialMediaUsersDataset.csv")

users_docs = []

for _, row in users_df.iterrows():

    try:
        age = datetime.now().year - datetime.strptime(
            row["DOB"],
            "%Y-%m-%d"
        ).year
    except:
        age = None

    users_docs.append({
        "user_id": int(row["UserID"]),
        "name": row["Name"],
        "gender": row["Gender"],
        "age": age,
        "city": row["City"],
        "country": row["Country"]
    })

db.users.insert_many(users_docs)

print(f"Inserted {len(users_docs)} users")

# ==========================
# LOAD POSTS
# ==========================

print("Loading posts...")

posts_df = pd.read_csv("Facebook_data_txt.csv")

posts_docs = []

for _, row in posts_df.iterrows():

    posts_docs.append({
        "post_id": int(row["post_id"]),
        "timestamp": row["timestamp"],
        "category": row["category"],
        "sentiment": row["sentiment"],
        "language": row["language"],
        "likes": int(row["likes"]),
        "comments": int(row["comments"]),
        "shares": int(row["shares"]),
        "is_spam": int(row["is_spam"])
    })

db.posts.insert_many(posts_docs)

print(f"Inserted {len(posts_docs)} posts")

# ==========================
# GENERATE FEEDS
# ==========================

print("Generating feeds...")

user_ids = users_df["UserID"].tolist()
post_ids = posts_df["post_id"].tolist()

feeds = []

feed_id = 1

for _ in range(1000):

    feeds.append({

        "feed_id": feed_id,

        "user_id": int(random.choice(user_ids)),

        "post_id": int(random.choice(post_ids)),

        "action": random.choice(
            ["view", "like", "share", "comment"]
        ),

        "timestamp": datetime.now()

    })

    feed_id += 1

db.feeds.insert_many(feeds)

print(f"Inserted {len(feeds)} feed events")

print("Pipeline completed successfully!")