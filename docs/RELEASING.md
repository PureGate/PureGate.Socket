# Releasing PureGate.Socket

Releases are published by hand with the **Publish Nuget Package** workflow ([.github/workflows/publish-nuget.yml](../.github/workflows/publish-nuget.yml)). It publishes the version in `VersionPrefix` of `src/PureGate.Socket/PureGate.Socket.csproj`.

## One-time setup

Publishing uses nuget.org [trusted publishing](https://learn.microsoft.com/nuget/nuget-org/trusted-publishing), so no API key is stored.

## Each release

1. **Set the version.** Put the release version in `VersionPrefix` in `src/PureGate.Socket/PureGate.Socket.csproj`. The workflow refuses a version that already has a `v<version>` tag.
2. **Declare the public API as shipped.** Move every line of `src/PureGate.Socket/PublicAPI/PublicAPI.Unshipped.txt` to the end of `PublicAPI.Shipped.txt` and leave Unshipped empty. The workflow refuses to publish while Unshipped has entries. After this, Unshipped lists only what changes after the release, so reviewers can tell new API from released API.
3. **Commit and push** to `main`.
4. **Publish:** Actions > Publish Nuget Package > Run workflow, on `main`.

The workflow builds, runs the tests, packs `PureGate.Socket.<version>.nupkg` with its symbols package (`.snupkg`), publishes both to nuget.org, then tags the commit `v<version>` and creates a GitHub release with the packages attached and generated notes. A version with a suffix, such as `1.1.0-rc.1`, is published as a prerelease.

CI builds prerelease packages of every commit (`<VersionPrefix>-ci.<run>`) as artifacts; they are never published.

## Versioning

Semantic versioning. Removing or changing anything in `PublicAPI.Shipped.txt` is a breaking change and needs a new major version; adding API is a minor version; fixes are a patch.
