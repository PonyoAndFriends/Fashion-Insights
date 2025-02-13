#!/bin/bash

set -e  # 스크립트 실행 중 오류 발생 시 즉시 종료

echo "🚀 1단계: ArgoCD 및 애플리케이션 삭제 시작..."
cd argo

echo "🛑 ArgoCD 애플리케이션 (Airflow, Spark, Kafka, ELK) 삭제 중..."
terraform destroy -auto-approve || echo "ArgoCD 관련 리소스가 이미 삭제되었거나 존재하지 않음."

cd ../gke

echo "🛑 2단계: GKE 및 인프라 삭제 시작..."
echo "🛑 Nginx Ingress Controller 삭제 중..."
helm uninstall nginx-ingress -n kube-system || echo "Nginx Ingress가 이미 삭제되었거나 존재하지 않음."

echo "🛑 Terraform을 이용한 GKE 및 VPC 삭제..."
terraform destroy -auto-approve || echo "GKE 및 VPC 관련 Terraform 리소스가 이미 삭제되었거나 존재하지 않음."

echo "🎉 전체 인프라 삭제 완료!"
