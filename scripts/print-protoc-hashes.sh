#!/usr/bin/env bash
set -eou pipefail

# Specify the protobuf release version
PROTOBUF_VERSION="29.2"

# Define the platforms
PLATFORMS=("linux-x86_64" "linux-aarch64" "darwin-aarch64")

# Array to store the hashes
declare -A HASHES

# Function to download and calculate the SHA-256 hash
calculate_hash() {
  local platform=$1
  local protoc_zip

  case "${platform}" in
  linux-x86_64)
    protoc_zip="protoc-${PROTOBUF_VERSION}-linux-x86_64.zip"
    ;;
  linux-aarch64)
    protoc_zip="protoc-${PROTOBUF_VERSION}-linux-aarch64.zip"
    ;;
  darwin-aarch64)
    protoc_zip="protoc-${PROTOBUF_VERSION}-osx-aarch_64.zip"
    ;;
  *)
    echo "Unsupported platform: ${platform}"
    exit 1
    ;;
  esac

  # Download the specified version of protobuf
  download_url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/${protoc_zip}"
  echo "Downloading from: ${download_url}"
  curl --fail --retry 5 -LO "${download_url}"

  # Calculate the SHA-256 hash
  if command -v shasum >/dev/null 2>&1; then
    sha256_hash=$(shasum -a 256 "${protoc_zip}" | cut -d' ' -f1)
  else
    sha256_hash=$(sha256sum "${protoc_zip}" | cut -d' ' -f1)
  fi

  # Store the hash in the array
  HASHES["${platform}"]="${sha256_hash}"

  # Clean up the zip file
  rm "${protoc_zip}"
}

# Iterate over the platforms and calculate the hashes
for platform in "${PLATFORMS[@]}"; do
  calculate_hash "${platform}"
done

# Print all the hashes together at the end
echo "Expected SHA-256 hashes for protobuf ${PROTOBUF_VERSION}:"
for platform in "${!HASHES[@]}"; do
  echo "[\"${platform}\"]=\"${HASHES[${platform}]}\""
done
