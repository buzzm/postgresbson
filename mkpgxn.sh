#!/bin/sh
#
# Build the PGXN distribution zip from a release tag.
#
#   ./mkpgxn.sh v2.2.1
#
# The zip is made with "git archive" from the tag, never from the working
# directory, so uncommitted edits and the scratch files lying around here
# cannot get into it, and every upload maps to exactly one commit.  Zip
# timestamps come from that commit, so rebuilding a tag gives the same files.
#
# Release steps:
#   1. Set META.json's top level "version" to X.Y.Z and commit.
#   2. git tag -a vX.Y.Z -m "PGXN X.Y.Z"
#   3. ./mkpgxn.sh vX.Y.Z
#   4. git push origin main vX.Y.Z
#   5. Upload bson-X.Y.Z.zip to PGXN.
# A tag may be moved (git tag -f) until it is pushed.  Once pushed, never move
# it: packagers build from it.  Release X.Y.Z+1 instead.
#
# Before building, these are checked at the tag (see CLAUDE.md for why):
#   - the tag is vX.Y.Z where X.Y.Z is META.json's top level "version"
#   - META.json provides.bson.version is pgbson.control's default_version
#     plus ".0" (PGXN wants three part versions; postgres uses two)
#   - provides.bson.file is pgbson--<default_version>.sql and exists
#
# Flat layout (no top level directory), matching every upload since 2.0.2.
# The file list is explicit on purpose; pgbson--*.sql picks up install and
# upgrade scripts automatically.
#
set -eu

die() { echo "mkpgxn.sh: $*" >&2; exit 1; }

[ $# -eq 1 ] || die "usage: $0 vX.Y.Z"
TAG=$1

command -v jq >/dev/null || die "jq is required"
cd "$(git rev-parse --show-toplevel)"

git rev-parse -q --verify "refs/tags/$TAG^{commit}" >/dev/null \
    || die "no tag $TAG (create it with: git tag -a $TAG -m \"PGXN ${TAG#v}\")"

at() { git show "$TAG:$1" 2>/dev/null || die "$1 missing at $TAG"; }

VERSION=$(at META.json | jq -er .version)
PVERSION=$(at META.json | jq -er .provides.bson.version)
PFILE=$(at META.json | jq -er .provides.bson.file)
EXTVER=$(at pgbson.control | sed -n "s/^default_version *= *'\(.*\)'.*/\1/p")

[ -n "$EXTVER" ] || die "cannot read default_version from pgbson.control at $TAG"
[ "$TAG" = "v$VERSION" ] \
    || die "tag $TAG does not match META.json version $VERSION at that commit"
[ "$PVERSION" = "$EXTVER.0" ] \
    || die "META.json provides.bson.version is $PVERSION, expected $EXTVER.0 (default_version $EXTVER)"
[ "$PFILE" = "pgbson--$EXTVER.sql" ] \
    || die "META.json provides.bson.file is $PFILE, expected pgbson--$EXTVER.sql"
at "$PFILE" >/dev/null

SQL=$(git ls-tree --name-only "$TAG" | grep '^pgbson--.*\.sql$') \
    || die "no pgbson--*.sql at $TAG"

ZIP=bson-$VERSION.zip
[ ! -e "$ZIP" ] || die "$ZIP already exists; move it aside first"

git archive --format=zip -o "$ZIP" "$TAG" -- \
    LICENSE \
    META.json \
    Makefile \
    README.md \
    pgbson.c \
    pgbson.control \
    pgbson_test.py \
    t1.sql \
    $SQL

echo "$ZIP built from $TAG ($(git rev-parse --short "$TAG^{commit}")):"
unzip -l "$ZIP"

if ! git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1; then
    echo
    echo "$TAG is not on origin yet.  Before uploading: git push origin main $TAG"
fi
