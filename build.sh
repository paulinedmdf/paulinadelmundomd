#!/usr/bin/env bash
# Vercel build script: install Quarto, render site.
# Site content is pure markdown — no R/Python execution needed at build time.
# Project notebooks under projects/ are pre-rendered and copied as static
# resources (see _quarto.yml).
set -euo pipefail

QUARTO_VERSION="1.9.37"
QUARTO_DIR="$HOME/.local/quarto-${QUARTO_VERSION}"

if [ ! -x "${QUARTO_DIR}/bin/quarto" ]; then
  echo "Installing Quarto ${QUARTO_VERSION}..."
  mkdir -p "$HOME/.local"
  curl -fsSL "https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-amd64.tar.gz" -o /tmp/quarto.tar.gz
  tar -xzf /tmp/quarto.tar.gz -C "$HOME/.local"
  rm /tmp/quarto.tar.gz
fi

export PATH="${QUARTO_DIR}/bin:${PATH}"
quarto --version
quarto render
