#!/bin/bash

set -e  # 스크립트 실행 중 오류 발생 시 즉시 종료

echo "🚀 1단계: GKE 클러스터 및 인프라 배포 시작..."
cd gke
terraform init
terraform apply -auto-approve

echo "✅ GKE 클러스터 배포 완료!"
echo "🌐 GKE 클러스터 연결 설정 중..."

# GKE 클러스터를 kubectl이 바라보도록 설정
GKE_CLUSTER_NAME=$(terraform output -raw kubernetes_cluster_name)
GKE_REGION=$(terraform output -raw region)
gcloud container clusters get-credentials $GKE_CLUSTER_NAME --region $GKE_REGION

echo "🔄 2단계: 필수 애드온 설치 (VPC CNI, CoreDNS, Nginx Ingress)"
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm install nginx-ingress ingress-nginx/ingress-nginx --namespace kube-system --set controller.service.type=LoadBalancer

echo "✅ 필수 애드온 설치 완료!"
echo "🚀 3단계: ArgoCD 배포 시작..."

cd ../argo
terraform init
terraform apply -auto-approve

echo "✅ ArgoCD 배포 완료!"
echo "🌍 ArgoCD UI 접속을 위한 포트포워딩 실행 중..."
kubectl port-forward svc/argocd-server -n argocd 8088:443 &

echo "🎉 전체 배포 완료! ArgoCD UI: https://localhost:8088"
