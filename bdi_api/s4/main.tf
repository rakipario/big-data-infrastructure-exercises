provider "aws" {
  region = "us-west-2" 
}

resource "aws_instance" "app" {
  ami           = "ami-0c55b159cbfafe1f0"  # Amazon Linux 2 AMI (us-west-2)
  instance_type = "t2.micro"
  key_name      = "my-key"  # Replace with your key pair name
  security_groups = [aws_security_group.app_sg.name]
  iam_instance_profile = aws_iam_instance_profile.s3_access.name

  user_data = <<-EOF
              #!/bin/bash
              sudo yum update -y
              sudo yum install -y python3 git
              curl -sSL https://install.python-poetry.org | python3 -
              export PATH="/home/ec2-user/.local/bin:$PATH"
              git clone https://github.com/rakipario/big-data-infrastructure-exercises/tree/main/bdi_api/s4 /home/ec2-user/bigdata
              cd /home/ec2-user/bigdata
              poetry install
              export BDI_S3_BUCKET=${var.s3_bucket}
              nohup poetry run uvicorn main:app --host 0.0.0.0 --port 8000 &
              EOF

  tags = {
    Name = "BDI-App"
  }
}

resource "aws_security_group" "app_sg" {
  name = "app-sg"
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Restrict to your IP for security
  }
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "s3_access" {
  name = "s3-access-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "s3_access_policy" {
  role = aws_iam_role.s3_access.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Effect = "Allow"
      Resource = ["arn:aws:s3:::${var.s3_bucket}", "arn:aws:s3:::${var.s3_bucket}/*"]
    }]
  })
}

resource "aws_iam_instance_profile" "s3_access" {
  name = "s3-access-profile"
  role = aws_iam_role.s3_access.name
}

variable "s3_bucket" {
  type = string
  description = "The name of the S3 bucket"
}