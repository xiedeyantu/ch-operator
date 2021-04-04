#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/..")"
TAG="xiedeyantu/ch-operator:v0.0.1"
DOCKERFILE="./Dockerfile"
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN:-xiedeyantu}"

if docker build -t "${TAG}" -f "${DOCKERFILE}" "${SRC_ROOT}"; then
    echo "SUCCEED docker build! "
    if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
        if [[ ! -z "${DOCKERHUB_LOGIN}" ]]; then
            echo "Dockerhub login specified: '${DOCKERHUB_LOGIN}', perform login"
            docker login -u "${DOCKERHUB_LOGIN}"
        fi
        docker push "${TAG}"
    fi
else
    echo "FAILED docker build! Abort."
    exit 1
fi