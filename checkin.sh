REPO_NAME=pathway
ECR_URL=789267064511.dkr.ecr.us-east-2.amazonaws.com

aws --region us-east-2 ecr get-login-password | docker login --username AWS --password-stdin ${ECR_URL}

docker build -t pathway .
docker tag ${REPO_NAME}:latest "${ECR_URL}/${REPO_NAME}:latest"
docker push ${ECR_URL}/${REPO_NAME}:latest