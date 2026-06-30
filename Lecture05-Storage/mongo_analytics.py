from pymongo import MongoClient
from pprint import pprint

client = MongoClient(
    "mongodb://admin:secret@localhost:27017/"
)

db = client["socialmedia_db"]

# print("="*60)
# print("TOP 5 MOST ACTIVE USERS")
# print("="*60)

# pipeline = [
#     {
#         "$group": {
#             "_id": "$user_id",
#             "total_actions": {"$sum": 1}
#         }
#     },
#     {
#         "$sort": {"total_actions": -1}
#     },
#     {
#         "$limit": 5
#     }
# ]

# for doc in db.feeds.aggregate(pipeline):
#     pprint(doc)

# print("\n")

print("="*60)
print("ACTION DISTRIBUTION")
print("="*60)

pipeline = [
    {
        "$group": {
            "_id": "$action",
            "count": {"$sum": 1}
        }
    }
]

for doc in db.feeds.aggregate(pipeline):
    pprint(doc)

print("\n")

print("="*60)
print("TOP POST CATEGORIES")
print("="*60)

pipeline_categories = [
    {
        "$group": {
            "_id": "$category",
            "posts": {"$sum": 1}
        }
    },
    {
        "$sort": {"posts": -1}
    },
    {
        "$limit": 5
    }
]

for doc in db.posts.aggregate(pipeline_categories):
    pprint(doc)

print("\n")
print("="*60)
print("TOP 5 MOST ACTIVE USERS WITH DETAILS")
print("="*60)

pipeline = [
    {
        "$group": {
            "_id": "$user_id",
            "total_actions": {"$sum": 1}
        }
    },
    {
        "$sort": {"total_actions": -1}
    },
    {
        "$limit": 5
    },
    {
        # JOIN sang collection 'users' để lấy thông tin chi tiết
        "$lookup": {
            "from": "users",
            "localField": "_id",       # _id ở đây hiện tại là user_id sau khi group
            "foreignField": "user_id",
            "as": "user_info"
        }
    },
    {
        "$unwind": "$user_info" # Trải phẳng mảng thông tin user
    },
    {
        # Định dạng lại kết quả đầu ra cho đẹp mắt, dễ nhìn
        "$project": {
            "user_id": "$_id",
            "name": "$user_info.name",
            "country": "$user_info.country",
            "total_actions": 1,
            "_id": 0
        }
    }
]

for doc in db.feeds.aggregate(pipeline):
    pprint(doc)