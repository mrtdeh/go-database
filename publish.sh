#!/bin/bash
git add .
git commit -m"update"
git push

echo "Publish $1"
git tag $1
git push origin $1

gh release create $1 --notes ""