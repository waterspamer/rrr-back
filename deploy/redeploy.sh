#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
RELEASE_DIR="${DEPLOY_RELEASE_DIR:-/root/rrr-back-release}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
DEPLOY_REPO_URL="${DEPLOY_REPO_URL:-$(git -C "$PROJECT_DIR" config --get remote.origin.url || printf '%s' 'https://github.com/waterspamer/rrr-back.git')}"
ENV_SOURCE_FILE="${DEPLOY_ENV_FILE:-$PROJECT_DIR/.env}"

if [ ! -f "$ENV_SOURCE_FILE" ]; then
  echo "Missing env file: $ENV_SOURCE_FILE" >&2
  exit 1
fi

if [ "$RELEASE_DIR" = "$PROJECT_DIR" ]; then
  echo "DEPLOY_RELEASE_DIR must not point at the mutable server checkout" >&2
  exit 1
fi

rm -rf "$RELEASE_DIR"
git clone --branch "$DEPLOY_BRANCH" --depth 1 "$DEPLOY_REPO_URL" "$RELEASE_DIR"

cp "$ENV_SOURCE_FILE" "$RELEASE_DIR/.env"

cd "$RELEASE_DIR"

if [ -d .git ]; then
  git rev-parse HEAD >/dev/null
fi

docker rm -f rrr-backend >/dev/null 2>&1 || true
docker compose up -d --build
