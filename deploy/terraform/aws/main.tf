# Lakeforge on AWS: VPC + EKS + RDS Postgres + S3 (workspace root) + IRSA,
# then the Helm chart. `terraform apply` from zero to a running workspace.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 5.60" }
    helm       = { source = "hashicorp/helm", version = "~> 2.14" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.31" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "aws" {
  region = var.region
}

locals {
  name = var.name
  tags = merge({ Project = "lakeforge", ManagedBy = "terraform" }, var.tags)
  azs  = slice(data.aws_availability_zones.available.names, 0, 3)
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# --------------------------------------------------------------------- vpc --
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name = local.name
  cidr = var.vpc_cidr

  azs             = local.azs
  private_subnets = [for i in range(3) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i in range(3) : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  enable_nat_gateway = true
  single_nat_gateway = true

  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }
  tags                = local.tags
}

# --------------------------------------------------------------------- eks --
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = local.name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true
  enable_irsa                              = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_addons = {
    coredns            = {}
    kube-proxy         = {}
    vpc-cni            = {}
    aws-ebs-csi-driver = { service_account_role_arn = module.ebs_csi_irsa.iam_role_arn }
  }

  eks_managed_node_groups = {
    control = {
      instance_types = [var.control_plane_instance_type]
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      labels         = { "lakeforge.io/pool" = "control" }
    }
    compute = {
      instance_types = [var.compute_instance_type]
      min_size       = var.compute_min_nodes
      max_size       = var.compute_max_nodes
      desired_size   = var.compute_min_nodes
      labels         = { "lakeforge.io/pool" = "compute" }
    }
  }

  tags = local.tags
}

module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name             = "${local.name}-ebs-csi"
  attach_ebs_csi_policy = true
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
  tags = local.tags
}

# ---------------------------------------------------------------- storage --
resource "aws_s3_bucket" "workspace" {
  bucket        = "${local.name}-workspace-${data.aws_caller_identity.current.account_id}"
  force_destroy = var.force_destroy_storage
  tags          = local.tags
}

resource "aws_s3_bucket_versioning" "workspace" {
  bucket = aws_s3_bucket.workspace.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "workspace" {
  bucket = aws_s3_bucket.workspace.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "workspace" {
  bucket                  = aws_s3_bucket.workspace.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "workspace_rw" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.workspace.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.workspace.arn}/*"]
  }
}

resource "aws_iam_policy" "workspace_rw" {
  name   = "${local.name}-workspace-rw"
  policy = data.aws_iam_policy_document.workspace_rw.json
  tags   = local.tags
}

# IRSA roles: control plane (api pod) and Forge compute pods.
module "control_plane_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name        = "${local.name}-control-plane"
  role_policy_arns = { s3 = aws_iam_policy.workspace_rw.arn }
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["${var.namespace}:lakeforge"]
    }
  }
  tags = local.tags
}

module "compute_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name        = "${local.name}-forge-compute"
  role_policy_arns = { s3 = aws_iam_policy.workspace_rw.arn }
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["lakeforge-compute:forge"]
    }
  }
  tags = local.tags
}

# --------------------------------------------------------------- database --
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "aws_db_subnet_group" "db" {
  name       = local.name
  subnet_ids = module.vpc.private_subnets
  tags       = local.tags
}

resource "aws_security_group" "db" {
  name   = "${local.name}-db"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = local.tags
}

resource "aws_db_instance" "db" {
  identifier              = local.name
  engine                  = "postgres"
  engine_version          = "16"
  instance_class          = var.db_instance_class
  allocated_storage       = 50
  max_allocated_storage   = 500
  storage_encrypted       = true
  db_name                 = "lakeforge"
  username                = "lakeforge"
  password                = random_password.db.result
  db_subnet_group_name    = aws_db_subnet_group.db.name
  vpc_security_group_ids  = [aws_security_group.db.id]
  multi_az                = var.db_multi_az
  skip_final_snapshot     = var.force_destroy_storage
  deletion_protection     = !var.force_destroy_storage
  backup_retention_period = 7
  tags                    = local.tags
}

# ------------------------------------------------------------------- helm --
data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_name
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.this.token
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    token                  = data.aws_eks_cluster_auth.this.token
  }
}

module "lakeforge" {
  source = "../modules/lakeforge"

  namespace      = var.namespace
  cloud          = "aws"
  api_image      = var.api_image
  forge_image    = var.forge_image
  image_tag      = var.image_tag
  admin_user     = var.admin_user
  admin_password = var.admin_password
  public_url     = var.public_url

  database_url = "postgres://lakeforge:${random_password.db.result}@${aws_db_instance.db.address}:5432/lakeforge"
  storage_root = "s3://${aws_s3_bucket.workspace.bucket}/workspace"
  storage_env  = { AWS_REGION = var.region, AWS_DEFAULT_REGION = var.region }

  control_plane_sa_annotations = { "eks.amazonaws.com/role-arn" = module.control_plane_irsa.iam_role_arn }
  compute_sa_annotations       = { "eks.amazonaws.com/role-arn" = module.compute_irsa.iam_role_arn }

  service_type = "LoadBalancer"
  service_annotations = {
    "service.beta.kubernetes.io/aws-load-balancer-type"   = "nlb"
    "service.beta.kubernetes.io/aws-load-balancer-scheme" = "internet-facing"
  }
  extra_values = merge({ forge = { nodeSelector = { "lakeforge.io/pool" = "compute" } } }, var.extra_helm_values)

  depends_on = [module.eks]
}
