#!/bin/bash

IMAGE_NAME="gitea-cdevents-adapter:latest"
TAR_FILE="gitea-cdevents-adapter.tar"

echo "Building image with Podman..."
podman build -t "$IMAGE_NAME" .

echo "Saving image to $TAR_FILE..."
podman save -o "$TAR_FILE" "$IMAGE_NAME"

echo "Loading image into Kind cluster..."
kind load image-archive "$TAR_FILE"

echo "Cleaning up..."
rm -f "$TAR_FILE"

echo "Image successfully loaded in Kind cluster!"
