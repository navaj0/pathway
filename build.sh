REPO_NAME=pathway-miniconda
ECR_URL=789267064511.dkr.ecr.us-east-2.amazonaws.com

set -e

tar cvzf .pathway.tgz ./src ./setup.py

aws --region us-east-2 ecr get-login-password | docker login --username AWS --password-stdin ${ECR_URL}

docker build -t pathway-miniconda .
docker tag ${REPO_NAME}:latest "${ECR_URL}/${REPO_NAME}:latest"
docker push ${ECR_URL}/${REPO_NAME}:latest