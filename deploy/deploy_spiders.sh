REPO_ROOT=$(git rev-parse --show-toplevel)
cd $REPO_ROOT/spider
scrapyd-deploy local-target -p morizon_spider

