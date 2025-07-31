# PyMongo Async Tutorial 요약

이 문서는 PyMongo의 비동기 API를 사용하여 MongoDB와 작업하는 방법에 대한 포괄적인 가이드입니다.

## 사전 요구사항

- PyMongo 설치 필요
- MongoDB 인스턴스가 기본 호스트와 포트에서 실행 중이어야 함

```python
import pymongo  # 예외 없이 실행되어야 함
```

## 1. AsyncMongoClient로 연결하기

### 기본 연결
```python
from pymongo import AsyncMongoClient

# 기본 호스트와 포트로 연결
client = AsyncMongoClient()

# 호스트와 포트 명시적으로 지정
client = AsyncMongoClient("localhost", 27017)

# MongoDB URI 형식 사용
client = AsyncMongoClient("mongodb://localhost:27017/")
```

### 명시적 연결
```python
# 첫 번째 작업 전에 명시적으로 연결
client = await AsyncMongoClient().aconnect()
```

## 2. 데이터베이스 가져오기

```python
# 속성 스타일 접근
db = client.test_database

# 딕셔너리 스타일 접근 (특수 문자가 포함된 이름의 경우)
db = client["test-database"]
```

## 3. 컬렉션 가져오기

```python
# 속성 스타일 접근
collection = db.test_collection

# 딕셔너리 스타일 접근
collection = db["test-collection"]
```

**중요:** 컬렉션과 데이터베이스는 지연 생성됩니다. 첫 번째 문서가 삽입될 때까지 실제로 생성되지 않습니다.

## 4. 문서 (Documents)

MongoDB의 데이터는 JSON 스타일 문서로 표현되며, PyMongo에서는 딕셔너리를 사용합니다.

```python
import datetime

post = {
    "author": "Mike",
    "text": "My first blog post!",
    "tags": ["mongodb", "python", "pymongo"],
    "date": datetime.datetime.now(tz=datetime.timezone.utc),
}
```

## 5. 문서 삽입

### 단일 문서 삽입
```python
posts = db.posts
post_id = (await posts.insert_one(post)).inserted_id
print(post_id)  # ObjectId('...')
```

### 대량 삽입
```python
new_posts = [
    {
        "author": "Mike",
        "text": "Another post!",
        "tags": ["bulk", "insert"],
        "date": datetime.datetime(2009, 11, 12, 11, 14),
    },
    {
        "author": "Eliot",
        "title": "MongoDB is fun",
        "text": "and pretty easy too!",
        "date": datetime.datetime(2009, 11, 10, 10, 45),
    },
]

result = await posts.insert_many(new_posts)
print(result.inserted_ids)  # [ObjectId('...'), ObjectId('...')]
```

## 6. 문서 조회

### 단일 문서 조회 (find_one)
```python
import pprint

# 첫 번째 문서 조회
pprint.pprint(await posts.find_one())

# 특정 조건으로 조회
pprint.pprint(await posts.find_one({"author": "Mike"}))

# ObjectId로 조회
pprint.pprint(await posts.find_one({"_id": post_id}))
```

### ObjectId 문자열 변환
```python
from bson.objectid import ObjectId

# 웹 프레임워크에서 URL로부터 post_id를 문자열로 받는 경우
async def get(post_id):
    document = await client.db.collection.find_one({'_id': ObjectId(post_id)})
```

### 여러 문서 조회 (find)
```python
# 모든 문서 조회
async for post in posts.find():
    pprint.pprint(post)

# 특정 조건으로 조회
async for post in posts.find({"author": "Mike"}):
    pprint.pprint(post)
```

## 7. 문서 개수 세기

```python
# 전체 문서 개수
count = await posts.count_documents({})

# 특정 조건에 맞는 문서 개수
count = await posts.count_documents({"author": "Mike"})
```

## 8. 범위 쿼리 및 정렬

```python
import datetime

d = datetime.datetime(2009, 11, 12, 12)

# 특정 날짜보다 이전 문서를 author로 정렬하여 조회
async for post in posts.find({"date": {"$lt": d}}).sort("author"):
    pprint.pprint(post)
```

## 9. 인덱싱

### 고유 인덱스 생성
```python
# user_id에 고유 인덱스 생성
result = await db.profiles.create_index([("user_id", pymongo.ASCENDING)], unique=True)

# 인덱스 정보 확인
sorted(list(await db.profiles.index_information()))
# ['_id_', 'user_id_1']
```

### 인덱스 활용
```python
# 사용자 프로필 삽입
user_profiles = [
    {"user_id": 211, "name": "Luke"}, 
    {"user_id": 212, "name": "Ziltoid"}
]
result = await db.profiles.insert_many(user_profiles)

# 고유 인덱스로 인한 중복 키 오류
new_profile = {"user_id": 213, "name": "Drew"}
duplicate_profile = {"user_id": 212, "name": "Tommy"}

result = await db.profiles.insert_one(new_profile)  # 성공
# result = await db.profiles.insert_one(duplicate_profile)  # DuplicateKeyError 발생
```

## 10. 작업 취소 (Task Cancellation)

asyncio Task를 취소하면 PyMongo 작업이 치명적인 중단으로 처리됩니다. 취소된 Task와 관련된 모든 연결, 커서, 트랜잭션은 안전하게 닫히고 정리됩니다.

## 주요 비동기 메서드 요약

| 동기 메서드 | 비동기 메서드 | 설명 |
|------------|-------------|------|
| `insert_one()` | `await insert_one()` | 단일 문서 삽입 |
| `insert_many()` | `await insert_many()` | 여러 문서 삽입 |
| `find_one()` | `await find_one()` | 단일 문서 조회 |
| `find()` | `async for ... in find()` | 여러 문서 조회 |
| `count_documents()` | `await count_documents()` | 문서 개수 세기 |
| `create_index()` | `await create_index()` | 인덱스 생성 |
| `list_collection_names()` | `await list_collection_names()` | 컬렉션 목록 조회 |

## 사용 시 주의사항

1. **await 키워드**: 모든 데이터베이스 작업에 `await` 키워드 사용 필수
2. **async for**: `find()` 결과 반복 시 `async for` 사용
3. **ObjectId 변환**: 문자열로 받은 ObjectId는 `ObjectId()` 생성자로 변환 필요
4. **스키마 자유**: MongoDB는 스키마가 없으므로 문서마다 다른 필드를 가질 수 있음
5. **지연 생성**: 데이터베이스와 컬렉션은 첫 문서 삽입 시 생성됨

이 요약을 통해 PyMongo의 비동기 API를 효과적으로 활용할 수 있습니다.