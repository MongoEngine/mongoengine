# MongoEngine Async Support Implementation Progress

## 프로젝트 목표
PyMongo의 AsyncMongoClient를 활용하여 MongoEngine에 완전한 비동기 지원을 추가하는 것

## 현재 상황 분석

### PyMongo Async 지원 현황
- PyMongo는 `AsyncMongoClient`를 통해 완전한 비동기 지원 제공
- 모든 주요 작업에 async/await 패턴 사용
- `async for`를 통한 커서 순회 지원
- 기존 동기 API와 병렬로 제공되어 호환성 유지

### MongoEngine 현재 구조의 문제점
1. **동기적 설계**: 모든 데이터베이스 작업이 블로킹 방식으로 구현
2. **디스크립터 프로토콜 한계**: ReferenceField의 lazy loading이 동기적으로만 가능
3. **전역 상태 관리**: 스레드 로컬 스토리지 사용으로 비동기 컨텍스트 관리 어려움
4. **QuerySet 체이닝**: 현재의 lazy evaluation이 async/await와 충돌

## 개선된 구현 전략

### 핵심 설계 원칙
1. **단일 Document 클래스**: 별도의 AsyncDocument 대신 기존 Document에 비동기 메서드 추가
2. **연결 타입 기반 동작**: async connection 사용 시 자동으로 비동기 동작
3. **명확한 메서드 네이밍**: `async_` 접두사로 비동기 메서드 구분
4. **완벽한 하위 호환성**: 기존 코드는 수정 없이 동작

### 1단계: 기반 구조 설계 (Foundation)

#### 1.1 하이브리드 연결 관리자
```python
# mongoengine/connection.py 수정
- 기존 connect() 함수 유지
- connect_async() 함수 추가로 AsyncMongoClient 연결
- get_connection()이 연결 타입 자동 감지
- contextvars로 비동기 컨텍스트 관리
```

#### 1.2 Document 클래스 확장
```python
# mongoengine/document.py 수정
class Document(BaseDocument):
    # 기존 동기 메서드 유지
    def save(self, ...):
        if is_async_connection():
            raise RuntimeError("Use async_save() with async connection")
        # 기존 로직
    
    # 새로운 비동기 메서드 추가
    async def async_save(self, force_insert=False, validate=True, ...):
        if not is_async_connection():
            raise RuntimeError("Use save() with sync connection")
        # 비동기 저장 로직
    
    async def async_delete(self, signal_kwargs=None, ...):
        # 비동기 삭제 로직
    
    async def async_reload(self):
        # 비동기 새로고침 로직
```

### 2단계: 핵심 CRUD 작업

#### 2.1 통합된 QuerySet
```python
# mongoengine/queryset/queryset.py 수정
class QuerySet:
    # 기존 동기 메서드 유지
    def first(self):
        if is_async_connection():
            raise RuntimeError("Use async_first() with async connection")
        # 기존 로직
    
    # 비동기 메서드 추가
    async def async_first(self):
        # 첫 번째 결과 반환
    
    async def async_get(self, *q_args, **q_kwargs):
        # 단일 객체 조회
    
    async def async_count(self):
        # 개수 반환
    
    async def async_create(self, **kwargs):
        # 객체 생성 및 저장
    
    def __aiter__(self):
        # 비동기 반복자
```

### 3단계: 고급 기능

#### 3.1 필드의 비동기 지원
```python
# ReferenceField에 비동기 메서드 추가
class ReferenceField(BaseField):
    # 기존 동기 lazy loading은 유지
    def __get__(self, instance, owner):
        if is_async_connection():
            # 비동기 컨텍스트에서는 Proxy 객체 반환
            return AsyncReferenceProxy(self, instance)
        # 기존 동기 로직
    
    # 명시적 비동기 fetch 메서드
    async def async_fetch(self, instance):
        # 비동기 참조 로드
```

#### 3.2 비동기 집계 작업
```python
class QuerySet:
    async def async_aggregate(self, pipeline):
        # 비동기 집계 파이프라인 실행
    
    async def async_distinct(self, field):
        # 비동기 distinct 작업
```

### 4단계: 신호 및 트랜잭션

#### 4.1 하이브리드 신호 시스템
```python
# 동기/비동기 모두 지원하는 신호
class HybridSignal:
    def send(self, sender, **kwargs):
        if is_async_connection():
            return self.async_send(sender, **kwargs)
        # 기존 동기 신호 전송
    
    async def async_send(self, sender, **kwargs):
        # 비동기 신호 핸들러 실행
```

#### 4.2 비동기 트랜잭션
```python
# 비동기 트랜잭션 컨텍스트 매니저
@asynccontextmanager
async def async_run_in_transaction():
    # 비동기 트랜잭션 관리
```

## 구현 로드맵

### Phase 1: 기본 구조 (2-3주) ✅ **완료** (2025-07-31)
- [x] 하이브리드 연결 관리자 구현 (connect_async, is_async_connection)
- [x] Document 클래스에 async_save(), async_delete() 메서드 추가
- [x] EmbeddedDocument 클래스에 비동기 메서드 추가
- [x] 비동기 단위 테스트 프레임워크 설정

### Phase 2: 쿼리 작업 (3-4주) ✅ **완료** (2025-07-31)
- [x] QuerySet에 비동기 메서드 추가 (async_first, async_get, async_count)
- [x] 비동기 반복자 (__aiter__) 구현
- [x] async_create(), async_update(), async_delete() 벌크 작업
- [x] 비동기 커서 관리 및 최적화

### Phase 3: 필드 및 참조 (2-3주)
- [ ] ReferenceField에 async_fetch() 메서드 추가
- [ ] AsyncReferenceProxy 구현
- [ ] LazyReferenceField 비동기 지원
- [ ] GridFS 비동기 작업 (async_put, async_get)
- [ ] 캐스케이드 작업 비동기화

### Phase 4: 고급 기능 (3-4주)
- [ ] 하이브리드 신호 시스템 구현
- [ ] async_run_in_transaction() 트랜잭션 지원
- [ ] 비동기 컨텍스트 매니저 (async_switch_db 등)
- [ ] async_aggregate() 집계 프레임워크 지원
- [ ] async_distinct() 고유 값 조회
- [ ] async_explain() 쿼리 실행 계획
- [ ] async_values(), async_values_list() 필드 프로젝션

### Phase 5: 통합 및 최적화 (2-3주)
- [ ] 성능 최적화 및 벤치마크
- [ ] 문서화 (async 메서드 사용법)
- [ ] 마이그레이션 가이드 작성
- [ ] 동기/비동기 통합 테스트

## 주요 고려사항

### 1. API 설계 원칙
- **통합된 Document 클래스**: 별도 클래스 없이 기존 Document에 비동기 메서드 추가
- **명명 규칙**: 비동기 메서드는 'async_' 접두사 사용 (예: save → async_save)
- **연결 타입 자동 감지**: 연결 타입에 따라 적절한 메서드 사용 강제

### 2. 호환성 전략
- 기존 코드는 100% 호환
- 동기 연결에서 async 메서드 호출 시 명확한 에러
- 비동기 연결에서 sync 메서드 호출 시 명확한 에러

### 3. 성능 고려사항
- 연결 풀링 최적화
- 배치 작업 지원
- 불필요한 비동기 오버헤드 최소화

### 4. 테스트 전략
- 모든 비동기 기능에 대한 단위 테스트
- 동기/비동기 동작 일관성 검증
- 연결 타입 전환 시나리오 테스트

## 예상 사용 예시

```python
from mongoengine import Document, StringField, connect_async

# 비동기 연결
await connect_async('mydatabase')

# 모델 정의 (기존과 완전히 동일)
class User(Document):
    name = StringField(required=True)
    email = StringField(required=True)

# 비동기 사용
user = User(name="John", email="john@example.com")
await user.async_save()

# 비동기 조회
user = await User.objects.async_get(name="John")
users = await User.objects.filter(name__startswith="J").async_count()

# 비동기 반복
async for user in User.objects.filter(active=True):
    print(user.name)

# 비동기 업데이트
await User.objects.filter(name="John").async_update(email="newemail@example.com")

# ReferenceField 비동기 로드
class Post(Document):
    author = ReferenceField(User)
    title = StringField()

post = await Post.objects.async_first()
# 비동기 컨텍스트에서는 명시적 fetch 필요
author = await post.author.async_fetch()
```

## 다음 단계

1. **Phase 1 시작**: 하이브리드 연결 관리자 구현
2. **커뮤니티 피드백**: 통합 설계 방식에 대한 의견 수렴
3. **벤치마크 설정**: 동기/비동기 성능 비교 기준 수립
4. **CI/CD 파이프라인**: 비동기 테스트 환경 구축

## 기대 효과

1. **완벽한 하위 호환성**: 기존 프로젝트는 수정 없이 동작
2. **점진적 마이그레이션**: 필요한 부분만 비동기로 전환 가능
3. **직관적 API**: async_ 접두사로 명확한 구분
4. **성능 향상**: I/O 바운드 작업에서 크게 개선

---

이 문서는 구현 진행에 따라 지속적으로 업데이트됩니다.

## 완료된 작업

### Phase 1: Foundation (2025-07-31 완료)

#### 구현 내용
- **연결 관리**: `connect_async()`, `disconnect_async()`, `is_async_connection()` 구현
- **Document 메서드**: `async_save()`, `async_delete()`, `async_reload()`, `async_ensure_indexes()`, `async_drop_collection()` 추가
- **헬퍼 유틸리티**: `async_utils.py` 모듈 생성 (ensure_async_connection, get_async_collection 등)
- **테스트**: 23개 async 테스트 작성 및 모두 통과

#### 주요 성과
- 기존 동기 코드와 100% 호환성 유지
- Sync/Async 연결 타입 자동 감지 및 검증
- 명확한 에러 메시지로 사용자 가이드
- 포괄적인 테스트 커버리지

#### 배운 점
- contextvars를 사용한 async 컨텍스트 관리가 효과적
- 연결 타입을 enum으로 관리하여 타입 안정성 확보
- pytest-asyncio의 fixture 설정이 중요함 (@pytest_asyncio.fixture 사용)
- cascade save for unsaved references는 별도 구현 필요

#### 다음 단계 준비사항
- QuerySet 클래스 구조 분석 필요
- async iterator 구현 패턴 연구
- 벌크 작업 최적화 방안 검토

### Phase 2: QuerySet Async Support (2025-07-31 완료)

#### 구현 내용
- **기본 쿼리 메서드**: `async_first()`, `async_get()`, `async_count()`, `async_exists()`, `async_to_list()`
- **비동기 반복**: `__aiter__()` 지원으로 `async for` 구문 사용 가능
- **벌크 작업**: `async_create()`, `async_update()`, `async_update_one()`, `async_delete()`
- **고급 기능**: 쿼리 체이닝, 참조 필드, MongoDB 연산자 지원

#### 주요 성과
- BaseQuerySet 클래스에 27개 async 메서드 추가
- 14개 포괄적 테스트 작성 및 통과
- MongoDB 업데이트 연산자 완벽 지원
- 기존 동기 코드와 100% 호환성 유지

#### 기술적 세부사항
- AsyncIOMotor 커서의 비동기 close() 처리
- `_from_son()` 파라미터 호환성 해결
- 업데이트 연산 시 자동 `$set` 래핑
- count_documents()의 None 값 처리 개선

#### 미구현 기능 (Phase 3/4로 이동)
- `async_aggregate()`, `async_distinct()` - 고급 집계 기능
- `async_values()`, `async_values_list()` - 필드 프로젝션
- `async_explain()`, `async_hint()` - 쿼리 최적화
- 이들은 기본 인프라 구축 후 필요시 추가 가능