REPO_NAME=pathway-mamba
ECR_URL=789267064511.dkr.ecr.us-east-2.amazonaws.com

set -e

tar cvzf .pathway.tgz ./src ./setup.py

aws --region us-east-2 ecr get-login-password | docker login --username AWS --password-stdin ${ECR_URL}

docker build -f mamba.Dockerfile -t pathway-mamba .
docker tag ${REPO_NAME}:latest "${ECR_URL}/${REPO_NAME}:latest"
docker push ${ECR_URL}/${REPO_NAME}:latest