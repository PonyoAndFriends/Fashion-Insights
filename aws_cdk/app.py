from aws_cdk import (
    aws_eks as eks,
    aws_ec2 as ec2,
    aws_iam as iam,
    App,
    Stack,
    CfnOutput
)
from constructs import Construct


class EKSClusterStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # VPC 생성 (2개 AZ)
        vpc = ec2.Vpc(self, "MyVPC", max_azs=2)

        # EKS 클러스터 생성
        cluster = eks.Cluster(
            self,
            "MyEKSCluster",
            vpc=vpc,
            default_capacity=0,  # 관리형 노드 그룹 사용
            version=eks.KubernetesVersion.V1_22  # 예시로 Kubernetes 1.21 버전 사용
        )

        # IAM Role for EKS Nodes
        node_role = iam.Role(self, "NodeRole",
                             assumed_by=iam.ServicePrincipal(
                                 "ec2.amazonaws.com"),
                             managed_policies=[
                                 iam.ManagedPolicy.
                                 from_aws_managed_policy_name(
                                     "AmazonEKSWorkerNodePolicy"
                                 ),
                                 iam.ManagedPolicy.
                                 from_aws_managed_policy_name(
                                     "AmazonEC2ContainerRegistryReadOnly"
                                 ),
                                 iam.ManagedPolicy.
                                 from_aws_managed_policy_name(
                                     "AmazonEKS_CNI_Policy"
                                 ),
                             ])

        # IRSA 인증 추가
        eks_service_account = eks.ServiceAccount(self, "MyEksServiceAccount",
                                                 cluster=cluster,
                                                 name="my-eks-service-account")
        eks_service_account.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonS3FullAccess")
        )

        # 관리형 노드 그룹 추가
        cluster.add_nodegroup_capacity(
            "NodeGroup",
            instance_types=[ec2.InstanceType("t3.medium")],
            min_size=3,
            max_size=5,
            desired_size=3,
            node_role=node_role
        )

        # 클러스터 정보 출력
        CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        CfnOutput(self, "ClusterEndpoint", value=cluster.cluster_endpoint)


app = App()
EKSClusterStack(app, "EKSClusterStack")
app.synth()
