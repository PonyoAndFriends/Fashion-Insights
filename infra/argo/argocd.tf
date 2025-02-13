# ✅ ArgoCD 네임스페이스 생성
resource "kubernetes_namespace" "argocd" {
  metadata {
    name = "argocd"
  }
}

# ✅ Airflow 네임스페이스 생성
resource "kubernetes_namespace" "airflow" {
  metadata {
    name = "airflow"
  }
}

# ✅ Spark 네임스페이스 생성
resource "kubernetes_namespace" "spark" {
  metadata {
    name = "spark"
  }
}

# ✅ Kafka 네임스페이스 생성
resource "kubernetes_namespace" "kafka" {
  metadata {
    name = "kafka"
  }
}

# ✅ ELK 네임스페이스 생성
resource "kubernetes_namespace" "elk" {
  metadata {
    name = "elk"
  }
}

# ✅ ArgoCD 배포 (Helm)
resource "helm_release" "argocd" {
  name       = "argocd"
  namespace  = kubernetes_namespace.argocd.metadata.0.name
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-cd"
  version    = "5.10.2"
}

# ✅ ArgoCD가 GitHub Repository를 감시하도록 설정
resource "kubectl_manifest" "argocd_app_airflow" {
  yaml_body = <<YAML
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: airflow
  namespace: argocd
spec:
  destination:
    namespace: airflow  # ✅ 미리 생성된 네임스페이스 사용
    server: https://kubernetes.default.svc
  source:
    repoURL: "https://github.com/PonyoAndFriends/Fashion-Insights.git"
    path: "k8s/airflow"
    targetRevision: main
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
YAML
}

resource "kubectl_manifest" "argocd_app_spark" {
  yaml_body = <<YAML
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: spark
  namespace: argocd
spec:
  destination:
    namespace: spark  # ✅ 미리 생성된 네임스페이스 사용
    server: https://kubernetes.default.svc
  source:
    repoURL: "https://github.com/PonyoAndFriends/Fashion-Insights.git"
    path: "k8s/spark"
    targetRevision: main
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
YAML
}

resource "kubectl_manifest" "argocd_app_kafka" {
  yaml_body = <<YAML
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: kafka
  namespace: argocd
spec:
  destination:
    namespace: kafka  # ✅ 미리 생성된 네임스페이스 사용
    server: https://kubernetes.default.svc
  source:
    repoURL: "https://github.com/PonyoAndFriends/Fashion-Insights.git"
    path: "k8s/kafka"
    targetRevision: main
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
YAML
}

resource "kubectl_manifest" "argocd_app_elk" {
  yaml_body = <<YAML
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: elk
  namespace: argocd
spec:
  destination:
    namespace: elk  # ✅ 미리 생성된 네임스페이스 사용
    server: https://kubernetes.default.svc
  source:
    repoURL: "https://github.com/PonyoAndFriends/Fashion-Insights.git"
    path: "k8s/elk"
    targetRevision: main
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
YAML
}
