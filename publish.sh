#!/bin/bash
echo "Publish $1"
git tag $1
git push origin $1

gh release create $1 --notes ""