# Release Versioning & Process

### GraphAr Version Number Scheme

GraphAr releases are each assigned an incremental version number in the format `v<MAJOR>.<MINOR>.<PATCH>` base on the [Semantic Versioning](https://semver.org/). For example:

- `v0.1.0` - New feature released.
- `v0.1.2` - Second patch release upon the `v0.1.0` feature release.

Patch releases are generally fairly minor, primarily intended for fixes and therefore are fairly unlikely to cause breakages upon update.
Feature releases are generally larger, bringing new features in addition to fixes and enhancements. These releases have a greater chance of introducing breaking changes upon update, so it's worth checking for any notes in the [release notes](https://github.com/alibaba/GraphAr/releases).

### Release Planning Process

Each GraphAr release will have a [milestone](https://github.com/alibaba/GraphAr/milestones) created with issues & pull requests assigned to it to define what will be in that release. Milestones are built up then worked through until complete at which point, after some testing and documentation updates, the release will be deployed.

### Release Announcements

Feature releases, and some patch releases, will be accompanied by a release note on the [GitHub release page](https://github.com/alibaba/GraphAr/releases) which will provide additional detail on features, changes & updates. 
