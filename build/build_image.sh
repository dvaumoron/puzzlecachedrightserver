#!/usr/bin/env bash

go install

buildah from --name puzzlecachedrightserver-working-container scratch
buildah copy puzzlecachedrightserver-working-container $HOME/go/bin/puzzlecachedrightserver /bin/puzzlecachedrightserver
buildah config --env SERVICE_PORT=50051 puzzlecachedrightserver-working-container
buildah config --port 50051 puzzlecachedrightserver-working-container
buildah config --entrypoint '["/bin/puzzlecachedrightserver"]' puzzlecachedrightserver-working-container
buildah commit puzzlecachedrightserver-working-container puzzlecachedrightserver
buildah rm puzzlecachedrightserver-working-container
