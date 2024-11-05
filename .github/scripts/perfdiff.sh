BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
gh workflow run benchmark-local.yaml --ref $BRANCH_NAME -f c1=$BRANCH_NAME
