## airflow 컨테이너에 관한 내용을 작성하는 디렉토리입니다.

- airflow DAG 코드는 `airflow_dags` 디렉토리 내에 작성해주시면 감사하겠습니다.

- 각자 플랫폼에 맞추어 작성하시면 됩니다. 이후 pyspark 코드는 예제 파일, 도커 파일을 토대로 테스팅 해보실 수 있습니다.

- 별도로 dynamic dag 용 디렉토리, config 작업 등이 필요하시다면 적절한 디렉토리에 만드시면 깃헙 레포를 자동으로 airflow가 가져가게 됩니다.

- spark에 job을 제출하는 dag를 작성하실 때는 dockerfile에 의해서 pyspark 코드가 마운트되는 경로에 주의해주시면 감사하겠습니다.
