# Optional VPC for MWAA: 2 private subnets (2 AZs) + 1 public subnet + single NAT.
# Recommended instead of using default VPC public subnets (MWAA requires private subnets).
#
# Enable with: enable_mwaa = true, create_mwaa_network = true
# Leave mwaa_vpc_id and mwaa_private_subnet_ids empty when using this.

locals {
  create_mwaa_net = var.enable_mwaa && var.create_mwaa_network
  mwaa_azs        = slice(data.aws_availability_zones.mwaa_available.names, 0, 2)
}

data "aws_availability_zones" "mwaa_available" {
  state = "available"
}

resource "aws_vpc" "mwaa" {
  count = local.create_mwaa_net ? 1 : 0

  cidr_block           = var.mwaa_vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(
    var.mwaa_tags,
    { Name = "${var.bucket_name}-mwaa-vpc", Project = "search-keyword-performance" },
  )
}

resource "aws_internet_gateway" "mwaa" {
  count = local.create_mwaa_net ? 1 : 0

  vpc_id = aws_vpc.mwaa[0].id
  tags = {
    Name = "${var.bucket_name}-mwaa-igw"
  }
}

resource "aws_subnet" "mwaa_public" {
  count = local.create_mwaa_net ? 1 : 0

  vpc_id                  = aws_vpc.mwaa[0].id
  cidr_block              = cidrsubnet(var.mwaa_vpc_cidr, 8, 0)
  availability_zone       = local.mwaa_azs[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.bucket_name}-mwaa-public"
  }
}

resource "aws_subnet" "mwaa_private" {
  count = local.create_mwaa_net ? 2 : 0

  vpc_id                  = aws_vpc.mwaa[0].id
  cidr_block              = cidrsubnet(var.mwaa_vpc_cidr, 8, 10 + count.index)
  availability_zone       = local.mwaa_azs[count.index]
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.bucket_name}-mwaa-private-${count.index + 1}"
  }
}

resource "aws_eip" "mwaa_nat" {
  count = local.create_mwaa_net ? 1 : 0

  domain = "vpc"
  tags = {
    Name = "${var.bucket_name}-mwaa-nat-eip"
  }

  depends_on = [aws_internet_gateway.mwaa]
}

resource "aws_nat_gateway" "mwaa" {
  count = local.create_mwaa_net ? 1 : 0

  allocation_id = aws_eip.mwaa_nat[0].id
  subnet_id     = aws_subnet.mwaa_public[0].id

  tags = {
    Name = "${var.bucket_name}-mwaa-nat"
  }

  depends_on = [aws_internet_gateway.mwaa]
}

resource "aws_route_table" "mwaa_public" {
  count = local.create_mwaa_net ? 1 : 0

  vpc_id = aws_vpc.mwaa[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.mwaa[0].id
  }

  tags = {
    Name = "${var.bucket_name}-mwaa-public-rt"
  }
}

resource "aws_route_table_association" "mwaa_public" {
  count = local.create_mwaa_net ? 1 : 0

  subnet_id      = aws_subnet.mwaa_public[0].id
  route_table_id = aws_route_table.mwaa_public[0].id
}

resource "aws_route_table" "mwaa_private" {
  count = local.create_mwaa_net ? 1 : 0

  vpc_id = aws_vpc.mwaa[0].id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.mwaa[0].id
  }

  tags = {
    Name = "${var.bucket_name}-mwaa-private-rt"
  }
}

resource "aws_route_table_association" "mwaa_private" {
  count = local.create_mwaa_net ? 2 : 0

  subnet_id      = aws_subnet.mwaa_private[count.index].id
  route_table_id = aws_route_table.mwaa_private[0].id
}

locals {
  mwaa_vpc_id_effective = local.create_mwaa_net ? aws_vpc.mwaa[0].id : var.mwaa_vpc_id
  mwaa_private_subnet_ids_effective = local.create_mwaa_net ? [
    aws_subnet.mwaa_private[0].id,
    aws_subnet.mwaa_private[1].id,
  ] : var.mwaa_private_subnet_ids
}
