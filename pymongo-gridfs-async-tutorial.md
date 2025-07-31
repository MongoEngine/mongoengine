# PyMongo GridFS 비동기 API 요약

이 문서는 PyMongo의 GridFS 비동기 API를 사용하여 MongoDB에서 대용량 파일을 저장하고 검색하는 방법에 대한 포괄적인 가이드입니다.

## GridFS 개요

GridFS는 16MB BSON 문서 크기 제한을 초과하는 파일을 저장하고 검색하기 위한 사양입니다. GridFS는 멀티 문서 트랜잭션을 지원하지 않습니다. 파일을 단일 문서에 저장하는 대신, GridFS는 파일을 청크로 나누고 각 청크를 별도의 문서로 저장합니다.

### GridFS 특징
- 기본 청크 크기: 255KB
- 두 개의 컬렉션 사용: 파일 청크 저장용과 파일 메타데이터 저장용
- 파일 버전 관리 지원
- 파일 업로드 날짜 및 메타데이터 추적

## 주요 클래스

### 1. AsyncGridFS
기본적인 GridFS 인터페이스를 제공하는 클래스입니다.

### 2. AsyncGridFSBucket
GridFS 버킷을 처리하는 클래스로, 더 현대적이고 권장되는 API입니다.

## 클래스별 상세 사용법

## AsyncGridFS 사용법

### 연결 설정
```python
from pymongo import AsyncMongoClient
import gridfs.asynchronous

client = AsyncMongoClient()
db = client.test_database
fs = gridfs.asynchronous.AsyncGridFS(db)
```

### 주요 메서드

#### 1. 파일 저장 (put)
```python
# 바이트 데이터 저장
file_id = await fs.put(b"hello world", filename="test.txt")

# 파일 객체 저장 (read() 메서드가 있는 객체)
with open("local_file.txt", "rb") as f:
    file_id = await fs.put(f, filename="uploaded_file.txt")

# 메타데이터와 함께 저장
file_id = await fs.put(
    b"hello world",
    filename="test.txt",
    content_type="text/plain",
    author="John Doe"
)
```

#### 2. 파일 조회 (get)
```python
# file_id로 파일 내용 가져오기
grid_out = await fs.get(file_id)
contents = grid_out.read()

# 파일명으로 파일 가져오기 (최신 버전)
grid_out = await fs.get_last_version("test.txt")
contents = grid_out.read()

# 특정 버전의 파일 가져오기
grid_out = await fs.get_version("test.txt", version=0)  # 첫 번째 버전
```

#### 3. 파일 존재 확인 (exists)
```python
# file_id로 확인
exists = await fs.exists(file_id)

# 파일명으로 확인
exists = await fs.exists({"filename": "test.txt"})

# 키워드 인수로 확인
exists = await fs.exists(filename="test.txt")
```

#### 4. 파일 삭제 (delete)
```python
# file_id로 삭제
await fs.delete(file_id)
```

#### 5. 파일 검색 (find)
```python
# 모든 파일 조회
async for grid_out in fs.find():
    print(f"Filename: {grid_out.filename}, Size: {grid_out.length}")

# 특정 조건으로 파일 검색
async for grid_out in fs.find({"filename": "test.txt"}):
    data = grid_out.read()

# 복잡한 쿼리
async for grid_out in fs.find({"author": "John Doe", "content_type": "text/plain"}):
    print(f"File: {grid_out.filename}")
```

#### 6. 단일 파일 검색 (find_one)
```python
# 첫 번째 매치되는 파일 반환
grid_out = await fs.find_one({"filename": "test.txt"})
if grid_out:
    contents = grid_out.read()
```

#### 7. 파일 목록 조회 (list)
```python
# 모든 파일명 목록
file_names = await fs.list()
print(file_names)
```

## AsyncGridFSBucket 사용법

### 연결 설정
```python
from pymongo import AsyncMongoClient
from gridfs.asynchronous import AsyncGridFSBucket

client = AsyncMongoClient()
db = client.test_database
bucket = AsyncGridFSBucket(db)

# 커스텀 설정
bucket = AsyncGridFSBucket(
    db,
    bucket_name="my_bucket",  # 기본값: "fs"
    chunk_size_bytes=1024*1024,  # 기본값: 261120 (255KB)
    write_concern=None,
    read_preference=None
)
```

### 주요 메서드

#### 1. 파일 업로드

##### 스트림에서 업로드
```python
# 바이트 데이터 업로드
data = b"Hello, GridFS!"
file_id = await bucket.upload_from_stream(
    "test_file.txt",
    data,
    metadata={"author": "John", "type": "text"}
)

# 파일 객체에서 업로드
with open("local_file.txt", "rb") as f:
    file_id = await bucket.upload_from_stream("uploaded_file.txt", f)
```

##### ID 지정하여 업로드
```python
from bson import ObjectId

custom_id = ObjectId()
await bucket.upload_from_stream_with_id(
    custom_id,
    "my_file.txt",
    b"file content",
    metadata={"custom": "data"}
)
```

#### 2. 파일 다운로드

##### 스트림으로 다운로드
```python
# file_id로 다운로드
with open("downloaded_file.txt", "wb") as f:
    await bucket.download_to_stream(file_id, f)

# 파일명으로 다운로드 (최신 버전)
with open("downloaded_file.txt", "wb") as f:
    await bucket.download_to_stream_by_name("test_file.txt", f)
```

##### 다운로드 스트림 열기
```python
# file_id로 스트림 열기
grid_out = await bucket.open_download_stream(file_id)
contents = await grid_out.read()

# 파일명으로 스트림 열기
grid_out = await bucket.open_download_stream_by_name("test_file.txt")
contents = await grid_out.read()
```

#### 3. 파일 삭제
```python
# file_id로 삭제
await bucket.delete(file_id)

# 파일명으로 삭제 (모든 버전)
await bucket.delete_by_name("test_file.txt")
```

#### 4. 파일 이름 변경
```python
# file_id로 이름 변경
await bucket.rename(file_id, "new_filename.txt")

# 파일명으로 이름 변경
await bucket.rename_by_name("old_filename.txt", "new_filename.txt")
```

#### 5. 파일 검색
```python
# 모든 파일 조회
async for grid_out in bucket.find():
    print(f"ID: {grid_out._id}, Name: {grid_out.filename}")

# 조건부 검색
async for grid_out in bucket.find({"metadata.author": "John"}):
    print(f"File: {grid_out.filename}")
```

## 파일 버전 관리

GridFS는 파일 버전 관리를 지원합니다. 동일한 파일명으로 여러 파일을 업로드할 수 있으며, 버전 번호로 구분됩니다:

- `version=-1`: 가장 최근 업로드된 파일 (기본값)
- `version=-2`: 두 번째로 최근 업로드된 파일
- `version=0`: 첫 번째로 업로드된 파일
- `version=1`: 두 번째로 업로드된 파일

```python
# 특정 버전 다운로드
grid_out = await bucket.open_download_stream_by_name(
    "test_file.txt",
    revision=0  # 첫 번째 버전
)
```

## GridOut 객체 속성

파일 조회 시 반환되는 GridOut 객체는 다음 속성들을 제공합니다:

```python
grid_out = await bucket.open_download_stream(file_id)

# 파일 정보
print(f"ID: {grid_out._id}")
print(f"파일명: {grid_out.filename}")
print(f"크기: {grid_out.length} bytes")
print(f"청크 크기: {grid_out.chunk_size}")
print(f"업로드 날짜: {grid_out.upload_date}")
print(f"콘텐츠 타입: {grid_out.content_type}")
print(f"메타데이터: {grid_out.metadata}")

# 파일 읽기
contents = await grid_out.read()
```

## 오류 처리

```python
from gridfs.errors import NoFile, FileExists, CorruptGridFile

try:
    # 존재하지 않는 파일 조회
    grid_out = await bucket.open_download_stream_by_name("nonexistent.txt")
except NoFile:
    print("파일을 찾을 수 없습니다")

try:
    # 중복 ID로 파일 생성
    await bucket.upload_from_stream_with_id(
        existing_id, "duplicate.txt", b"content"
    )
except FileExists:
    print("동일한 ID의 파일이 이미 존재합니다")
```

## 세션 지원

GridFS 작업에서 세션을 사용할 수 있습니다:

```python
async with await client.start_session() as session:
    # 세션을 사용한 파일 업로드
    file_id = await bucket.upload_from_stream(
        "session_file.txt",
        b"content",
        session=session
    )
    
    # 세션을 사용한 파일 다운로드
    grid_out = await bucket.open_download_stream(file_id, session=session)
```

## 인덱스 관리

GridFS는 효율성을 위해 chunks와 files 컬렉션에 인덱스를 사용합니다:

- `chunks` 컬렉션: `files_id`와 `n` 필드에 대한 복합 고유 인덱스
- `files` 컬렉션: `filename`과 `uploadDate` 필드에 대한 인덱스

## 실제 사용 예제

### 이미지 파일 업로드 및 다운로드
```python
import asyncio
from pymongo import AsyncMongoClient
from gridfs.asynchronous import AsyncGridFSBucket

async def image_storage_example():
    client = AsyncMongoClient()
    db = client.photo_storage
    bucket = AsyncGridFSBucket(db)
    
    # 이미지 업로드
    with open("photo.jpg", "rb") as image_file:
        file_id = await bucket.upload_from_stream(
            "user_photo.jpg",
            image_file,
            metadata={
                "user_id": "12345",
                "upload_time": "2025-01-01",
                "content_type": "image/jpeg"
            }
        )
    
    print(f"이미지 업로드 완료: {file_id}")
    
    # 이미지 다운로드
    with open("downloaded_photo.jpg", "wb") as output_file:
        await bucket.download_to_stream(file_id, output_file)
    
    print("이미지 다운로드 완료")
    
    # 사용자별 이미지 검색
    async for grid_out in bucket.find({"metadata.user_id": "12345"}):
        print(f"사용자 이미지: {grid_out.filename}, 크기: {grid_out.length}")

# 실행
asyncio.run(image_storage_example())
```

## 주요 차이점: AsyncGridFS vs AsyncGridFSBucket

| 기능 | AsyncGridFS | AsyncGridFSBucket |
|------|-------------|-------------------|
| API 스타일 | 기본적인 key-value 인터페이스 | 현대적인 스트림 기반 API |
| 권장 사용 | 레거시 코드 | 새로운 프로젝트 |
| 파일 업로드 | `put()` | `upload_from_stream()` |
| 파일 다운로드 | `get()` | `download_to_stream()` |
| 스트림 지원 | 제한적 | 완전 지원 |
| 파일명 기반 작업 | 제한적 | 완전 지원 |

## 성능 최적화 팁

1. **청크 크기 조정**: 대용량 파일의 경우 청크 크기를 늘려 성능 향상
2. **인덱스 활용**: 파일 검색 시 메타데이터 필드에 인덱스 생성
3. **세션 사용**: 관련 작업들을 세션으로 그룹화
4. **스트림 API 사용**: 메모리 효율적인 파일 처리를 위해 스트림 API 활용

## 주의사항

1. GridFS는 멀티 문서 트랜잭션을 지원하지 않습니다
2. 파일의 전체 내용을 원자적으로 업데이트해야 하는 경우 GridFS를 사용하지 마세요
3. 모든 파일이 16MB 미만인 경우, GridFS 대신 단일 문서에 BinData로 저장하는 것을 고려하세요
4. 파일 삭제 중에는 동시 읽기를 피하세요

이 요약을 통해 PyMongo의 GridFS 비동기 API를 효과적으로 활용하여 MongoDB에서 대용량 파일을 관리할 수 있습니다.