#!/usr/bin/env bash

# Sync tags between upstream and origin

git fetch --tags upstream
git push --tags origin