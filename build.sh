#!/bin/sh

IMAGE=registry.gitlab.com/clgt/dtworker
VERSION=$(git describe --tags)

echo "build docker image for release: $VERSION"
docker build -t $IMAGE:$VERSION .
docker push $IMAGE:$VERSION