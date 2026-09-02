#!/bin/bash -e
# Prints the body for a draft GitHub release: a placeholder for the human
# readable summary, followed by the CHANGELOG.md section of the given tag.
die() {
	echo "$@" >&2
	exit 1
}

TAG="$1"
if [ -z "$TAG" ]; then
	die "Usage: $0 <tag>"
fi

# Changelog headers carry the bare version: "## [1.13.0] - 2026-09-02".
VERSION="${TAG#v}"

if ! grep -q "^## \[$VERSION\] -" CHANGELOG.md; then
	die "CHANGELOG.md has no section for $VERSION"
fi

echo "TODO: human readable release note"
echo ""
echo "---"
awk "/^## \[$VERSION\] -/{flag=1; next} /^## \[/{flag=0} flag" CHANGELOG.md
