## Pod로 실행할 파이썬 스크립트를 작성하시는 폴더입니다.

- python_scripts 폴더 내에 각자의 플랫폼 이름에 맞추어 폴더를 생성, 그 안에서 작업해주시면 감사하겠습니다.

- Dockerfile.k8spodimage 기반으로 빌드된 이미지는 CustomKubernetesPodOperator에서 쿠버네티스 내에 Pod를 생성할 때 가져가 쓰게 됩니다. 

- spark에 job을 제출하는 dag를 작성하실 때는 dockerfile에 의해서 pyspark 코드가 마운트되는 경로에 주의해주시면 감사하겠습니다.
