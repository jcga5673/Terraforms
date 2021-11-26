
resource "aws_redshift_cluster" "default" {
  cluster_identifier = var.cluster_identifier
  database_name      = var.database_name
  master_username    = var.master_username
  master_password    = var.master_password
  node_type          = var.node_type
  cluster_type       = var.cluster_type
  number_of_nodes    = var.number_of_nodes
  port = var.db_port_redshift
  skip_final_snapshot = var.skip_final_snapshot
  publicly_accessible     = var.publicly_accessible

  #cluster_subnet_group_name    = aws_redshift_subnet_group.foo.id
  #vpc_security_group_ids  = [aws_security_group.allow_tls.id]

  #tags = {
  #  Name = "redshift-database"
  #}
}


resource "aws_security_group" "allow_tls" {
  name        = "allow_tls"
  description = "Allow TLS inbound traffic"
  #vpc_id      = var.vpc_id_redshift

  ingress {
    description      = "TLS from VPC"
    from_port        = var.db_port_redshift
    to_port          = var.db_port_redshift
    protocol         = "tcp"
    cidr_blocks      = ["10.0.1.0/24","10.0.2.0/24"]#[aws_vpc.main.cidr_block]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
  }

  tags = {
    Name = "allow_tls"
  }
}




resource "aws_vpc" "foo" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "foo" {
  cidr_block        = "10.0.1.0/24"
  availability_zone = "us-east-2a"
  vpc_id            = aws_vpc.foo.id

  tags = {
    Name = "tf-dbsubnet-test-1"
  }
}

resource "aws_subnet" "bar" {
  cidr_block        = "10.0.2.0/24"
  availability_zone = "us-east-2b"
  vpc_id            = aws_vpc.foo.id

  tags = {
    Name = "tf-dbsubnet-test-2"
  }
}

resource "aws_redshift_subnet_group" "foo" {
  name       = "foo"
  subnet_ids = [aws_subnet.foo.id, aws_subnet.bar.id]

  tags = {
    environment = "Production"
  }
}