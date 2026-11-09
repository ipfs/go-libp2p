#!/usr/bin/env bash
set -eou pipefail

# This script requires bash 4+, some macOS installations have bash 3

# Specify the protobuf release version
PROTOBUF_VERSION="29.2"

# Define SHA-256 hashes for each supported platform
# Update these hashes by running ./print-protoc-hashes.sh
declare -A SHA256_HASHES=(
  ["linux-aarch64"]="0019dfc4b32d63c1392aa264aed2253c1e0c2fb09216f8e2cc269bbfb8bb49b5"
  ["linux-x86_64"]="52e9e7ece55c7e30e7e8bbd254b4b21b408a5309bca826763c7124b696a132e9"
  ["darwin-aarch64"]="0e153a38d6da19594c980e7f7cd3ea0ddd52c9da1068c03c0d8533369fbfeb20"
)

# Determine the platform
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
[[ "${ARCH}" == "arm64" ]] && ARCH="aarch64"

PLATFORM="${OS}-${ARCH}"

# Set the download URL based on the platform
case "${PLATFORM}" in
linux-x86_64)
  PROTOC_ZIP="protoc-${PROTOBUF_VERSION}-linux-x86_64.zip"
  ;;
linux-aarch64)
  PROTOC_ZIP="protoc-${PROTOBUF_VERSION}-linux-aarch64.zip"
  ;;
darwin-aarch64)
  PROTOC_ZIP="protoc-${PROTOBUF_VERSION}-osx-aarch_64.zip"
  ;;
*)
  echo "Unsupported platform: ${PLATFORM}" >&2
  exit 1
  ;;
esac

# Download the specified version of protobuf
DOWNLOAD_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/${PROTOC_ZIP}"
echo "Downloading from: ${DOWNLOAD_URL}" >&2
curl --fail --retry 5 -LO "${DOWNLOAD_URL}"

# Verify checksum
EXPECTED_SHA256="${SHA256_HASHES[${PLATFORM}]}"
if command -v shasum >/dev/null 2>&1; then
  ACTUAL_SHA256=$(shasum -a 256 "${PROTOC_ZIP}" | cut -d' ' -f1)
else
  ACTUAL_SHA256=$(sha256sum "${PROTOC_ZIP}" | cut -d' ' -f1)
fi

if [[ "${ACTUAL_SHA256}" != "${EXPECTED_SHA256}" ]]; then
  echo "Checksum verification failed!" >&2
  echo "Expected: ${EXPECTED_SHA256}" >&2
  echo "Got: ${ACTUAL_SHA256}" >&2
  rm "${PROTOC_ZIP}"
  exit 1
fi

echo "Checksum verified successfully" >&2

# Create a directory for extraction
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/protobuf-bin/protoc-${PROTOBUF_VERSION}"
mkdir -p "${INSTALL_DIR}"

# Unzip the downloaded file
unzip -q -o "${PROTOC_ZIP}" -d "${INSTALL_DIR}"

# Clean up the zip file
rm "${PROTOC_ZIP}"

# Return a new PATH with the protobuf binary
PROTOC_BIN="${INSTALL_DIR}/bin"
echo "Installed protoc ${PROTOBUF_VERSION} to ${INSTALL_DIR}" >&2

# Return the protoc bin path to stdout
printf "${PROTOC_BIN}"
