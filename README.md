# 한국 마사회 데이터 파이프라인

</br>
</br>

# ☑︎ 프로젝트 결과



![Screenshot 2023-02-01 오후 4.26.09.png](https://github.com/hubjh/korea-racing-authority/blob/master/Screenshot%202023-02-01%20%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE%204.26.09.png)

### 🔗 Link

[경마 누적상금 순위 💰︎💰︎🤑💰︎💰︎](https://www.youtube.com/shorts/m6n8JoBUgsA)

## **결과의 의미**

|  | 개인 | 팀 |
| --- | --- | --- |
| 얻은 것 |  | 데이터 플랫폼 서버의 테스트를 위한 데이터 파이프라인을 추가 |

</br>
</br>

# ✎ 프로젝트 배경



| 프로젝트 인원 | 2명 |
| :--- | :--- |
| 프로젝트의 목표 | 데이터 파이프라인 구축 경험 |
| 팀의 목표 | 데이터플랫폼 서버의 인프라 성능, 안정성 테스트</br>1. k8s에 pod로 올려놓은 Spark, Airflow 등이 안정적으로 작동하는지 테스트</br>2. k8s 노드들의 리소스(CPU, Memory) 이상 테스트</br>3. 이상 발견 시 문제점을 해결한다. |
| 팀 내 나의 역할  | 1. 공공데이터포털 API로 데이터(한국마사회 경주기록 정보, 한국마사회 말 정보 및 개체식별) 파이프라인 Python 스크립트 만들기</br>2. Airflow를 통해 Python 스크립트들의 스케줄 작성 |
| 팀의 문제 혹은 기회 상황 | 프로젝트</br>1. Spark, Airflow, k8s를 배우면서 만들어야 한다.</br>2. 데이터 플랫폼 서버의 구조를 다 파악하지 못했다.</br>3. 모르는 부분들은 팀장이 도움을 줄 수 있다. |
| 개발 기간 | 2022.10 ~ 2022.11 |
| 개발 언어  | Python |
| 개발 환경 | 온프레미스 환경 Ubuntu:20.04 |
| 사용 기술 | Main : 주로 사용한 기술</br></br>• Hadoop</br>    ◦ 수집한 데이터 소스와 Spark로 가공한 데이터 저장</br>• Docker</br>    ◦ 개발 단계에서 테스트용 컨테이너 생성</br>• Spark</br>    ◦ 수집한 데이터를 필터링 할 때 사용</br>• Airflow</br>    ◦ Spark 실행 스케쥴 관리 |

</br>
</br>

# 🧩 프로젝트 과정



### 조사내용

</br>


## Parquet로 저장하는 이유



### ⚔️ 행기반(Row-Based) vs 열기반(Columnar)

<aside>
🔖 **열기반(Columnar)**

</aside>

|  | 행 기반 포맷 | 열 기반 포맷 |
| --- | --- | --- |
| 데이터 구조 | 각 행을 순차적으로 저장 | 각 열을 순차적으로 저장 |
| 압축 효율 | 데이터 압축률이 낮음 | 데이터 압축률이 높음 |
| 데이터 읽기 | 특정 행을 읽을 때 해당 행에 포함된 모든 열을 읽어야 함 | 특정 열을 읽을 때 해당 열에 포함된 모든 행을 읽어야 함 |
| 데이터 쓰기 | 특정 행을 추가 또는 갱신하기 위해 전체 행을 다시 기록 | 특정 열을 추가 또는 갱신하기 위해 해당 열에 포함된 값만 기록 |
| 처리 성능 | 특정 행에 대한 조회 및 수정이 빠름 | 특정 열에 대한 조회 및 집계가 빠름 |
| 필드 선택 | 모든 필드를 읽어야 함 | 필요한 필드만 선택적으로 읽을 수 있음 |
| 데이터 분석 | 특정 행의 모든 필드에 대한 분석이 필요한 경우 | 특정 열에 대한 집계, 필터링 및 변환이 필요한 경우 |
| 적합한 사용 사례 | OLTP (온라인 트랜잭션 처리) 시스템, 로그 데이터 등 | 데이터 웨어하우스, 대규모 분석, 컬럼 기반 쿼리 등 |

HDFS는 대용량의 데이터를 저장하고 처리하기 위해 설계된 파일 시스템으로 행 기반 포맷을 사용하는 경우 읽기 성능이 저하될 수 있다.

### ⚔️ Parquet vs ORC vs Avro

<aside>
🔖 **Parquet**

</aside>

|  | Parquet | ORC | Avro |
| --- | --- | --- | --- |
| 데이터 구조 | 각 열을 순차적으로 저장 | 각 열을 순차적으로 저장 | 각 열을 순차적으로 저장 |
| 압축 효율 | 높음 | 높음 | 중간 |
| 스키마 유연성 | 스키마 유연성이 높음 | 스키마 유연성이 낮음 | 스키마 유연성이 높음 |
| 데이터 읽기 | 선택적 열 읽기 가능 | 선택적 열 읽기 가능 | 선택적 열 읽기 가능 |
| 데이터 쓰기 | 선택적 열 쓰기 가능 | 선택적 열 쓰기 가능 | 선택적 열 쓰기 가능 |
| 호환성 | 다양한 시스템 및 프레임워크와 호환됨 | 주로 Apache Hive와 호환됨 | 다양한 시스템 및 프레임워크와 호환됨 |
| 직렬화 형식 | 이진 포맷 (Binary Format) | 이진 포맷 (Binary Format) | 이진 및 텍스트 포맷 (Binary and Text Format) |
| 적합한 사용 사례 | 대규모 데이터 처리, 데이터 웨어하우스, 데이터 레이크 | 대규모 데이터 처리, 데이터 웨어하우스, 데이터 레이크 | 대규모 데이터 처리, 데이터 통합 |

ORC는 Hive에 특화된 열기반 포맷으로 Parquet이 범용성이 더 높다고 판단했다.

</br>
</br>

# 프로그램 동작



1. 매월 1일 0시 0분에 Python 스크립트 파일을 Airflow로 스케줄링하여 공공데이터포털API 데이터소스를 수집
2. 수집한 데이터를 Parquet로 가공 후 yellow 테이블로 관리
3. yellow 테이블에서 SQL로 얻은 데이터를 D3.js 차트 라이브러리로 시각화 후 유튜브에 업로드

</br>
</br>

## Airflow DAG



| schedule_interval | 0 0 1 * *  |
| --- | --- |
| DAG | Crawler >> Parser |

### Crawler

| Operator | KubernetesPodOperator |
| --- | --- |
| cmds | [‘python3’, ‘foo.py’, ‘1990-02-12’, ‘{{ next_ds }}’] |
| 기능 | 1. 한국마사회 경주기록 정보</br>2. 한국마사회 말 정보 및 개체식별 </br></br>2가지 API에서 월별 데이터 수집</br>HDFS에 JSON으로 포맷 |

### Parser

| Operator | KubernetesPodOperator |
| --- | --- |
| cmds | [‘/bin/spark-submit’, ‘bar.py’] |
| 기능 | 1. 한국마사회 경주기록 정보</br>2. 한국마사회 말 정보 및 개체식별 </br></br>red 테이블에서 필요한 데이터만 뽑아서 Spark로 중복 제거, 스키마 부여</br>HDFS에 parquet으로 포맷 |
