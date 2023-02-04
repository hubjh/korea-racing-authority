![Screenshot 2023-02-01 오후 4.26.09.png](https://github.com/hubjh/korea-racing-authority/blob/master/Screenshot%202023-02-01%20%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE%204.26.09.png)

---

### 💡 간단 소개

- 공공데이터포털에서 한국 마사회 데이터를 수집하여 유튜브에 동적 그래프로 영상을 업로드 해보는 개인 프로젝트

### 💻 개발 언어

- Python

### 📆 개발 기간

- 2022.10.29 ~ 2022.11.12

### ☀️ 개발 환경

- Linux

### 👨🏻‍💻 개발 인원

- 2명

### 🔧 담당한 역할

- **데이터 수집**
    - 매일 공공데이터포털에서 API 크롤링을 Airflow로 스케쥴링하여 데이터소스를 JSON 파일로 수집

- **데이터 가공**
    - 수집이 끝나면 바로 데이터를 Parquet로 가공 후 테이블로 관리

---

## 사용 기술

- **Hadoop**
    - 크롤링한 데이터 소스와 Spark로 처리한 데이터 저장
- **Docker**
    - 개발단계에서 테스트용 컨테이너 생성
- **Kubernetes**
    - Spark와 Airflow가 Kubernetes에서 실행
- **Spark**
    - 수집한 데이터를 처리할 때 사용
- **Airflow**
    - 데이터 수집, 데이터 처리 스케쥴 관리

---

### 🔗 Link

[경마 누적상금 순위 💰︎💰︎🤑💰︎💰︎](https://www.youtube.com/shorts/m6n8JoBUgsA)
