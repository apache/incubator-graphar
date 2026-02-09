# Contributing to GraphAr

First off, thank you for considering contributing to GraphAr. It's
people like you that make GraphAr better and better.

There are many ways to contribute, from improving the documentation,
submitting bug reports and feature requests or writing code which can
be incorporated into GraphAr itself.

- [Contributing to GraphAr](#contributing-to-graphar)
  - [Code of Conduct](#code-of-conduct)
  - [First Contribution](#first-contribution)
  - [Workflow](#workflow)
    - [Create a branch](#create-a-branch)
    - [GitHub Pull Requests](#github-pull-requests)
      - [Title](#title)
      - [Reviews & Approvals](#reviews--approvals)
      - [Merge Style](#merge-style)
      - [CI](#ci)
  - [Setup Development Environment](#setup-development-environment)
    - [Using a dev container environment](#using-a-dev-container-environment)
    - [Use your own toolkit](#use-your-own-toolkit)

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

## First Contribution

1. Ensure your change has an issue! Find an [existing issue](https://github.com/apache/graphar/issues) or [open a new issue](https://github.com/apache/graphar/issues/new).
1. [Fork the GraphAr repository](https://github.com/apache/graphar/fork) in your own GitHub account.
1. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
1. Make your changes.
1. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the main GraphAr repo. An GraphAr team member should comment and/or review your pull request within a few days. Although, depending on the circumstances, it may take longer.
1. Discussing and keeping your Pull Request updated. 

   You will probably get feedback or requests for changes to your pull request. This is a big part of the submission process so don't be discouraged! It is a necessary part of the process in order to evaluate whether the changes are correct and necessary.

   Feel free to post a comment in the pull request to ping reviewers if you are awaiting an answer on something. If you encounter words or acronyms that seem unfamiliar, refer to this [glossary](https://chromium.googlesource.com/chromiumos/docs/+/HEAD/glossary.md).

## Workflow

### Create a branch

*All* changes has to be made in a branch and submitted as [pull requests](#github-pull-requests). 

A good branch name would be the issue number you are working on and a short description of the change,
like (where issue \#42 is the ticket you're working on):

```shell
$ git checkout -b 42-add-chinese-translations
```
### pre-commit

Install the python package `pre-commit` and run once `pre-commit install`.

```
pip install pre-commit
pre-commit install
pre-commit run # check staged files
pre-commit run -a # check all files
```

This will set up a git pre-commit-hook that is executed on each commit and will report the linting problems.

### GitHub Pull Requests

Once your changes are ready you must submit your branch as a [pull request](
https://github.com/apache/graphar/pulls)

#### Title

The pull request title must follow the format outlined in the [conventional commits spec](https://www.conventionalcommits.org). 

[Conventional commits](https://www.conventionalcommits.org) is a standardized format for commit messages. 
GraphAr only requires this format for commits on the `main` branch. And because GraphAr squashes commits before merging branches, this means that only the pull request title must conform to this format.

The following are all good examples of pull request titles:

```text
feat(c++): Support timestamp data type
docs: fix the images link of README
ci: Mark job as skipped if owner is not apache
fix(c++): Fix the bug of the memory leak
refactor: Refactor the API of the vertex info implementation
```

#### Reviews & Approvals

All pull requests should be reviewed by at least one GraphAr committer.

#### Merge Style

All pull requests are squash merged.
We generally discourage large pull requests that are over 300–500 lines of diff.
If you would like to propose a change that is larger, we suggest
coming onto our [Discussions](https://github.com/apache/graphar/discussions) and discussing it with us.
This way we can talk through the solution and discuss if a change that large is even needed!
This will produce a quicker response to the change and likely produce code that aligns better with our process.

### CI

Currently, GraphAr uses GitHub Actions to run tests. The workflows are defined in `.github/workflows`.

## Setup Development Environment

For small or first-time contributions, we recommend the dev container method. And if you prefer to do it yourself, that's fine too!

### Using a dev container environment

GraphAr provides a pre-configured [dev container](https://containers.dev/)
that could be used in [VSCode](https://code.visualstudio.com/docs/devcontainers/containers), [JetBrains](https://www.jetbrains.com/remote-development/gateway/),
[JupyterLab](https://jupyterlab.readthedocs.io/en/stable/).
Please pick up your favorite runtime environment.

### Use your own toolkit

Different components of GraphAr may require different setup steps. Please refer to their respective `README` documentation for more details.

- [C++ Library](cpp/README.md)
- [Scala with Spark Library](maven-projects/spark/README.md)
- [Python with PySpark Library](pyspark/README.md) (under development)
- [Java Library](maven-projects/java/README.md) (under development)

----

This doc refer from [Apache OpenDAL](https://opendal.apache.org/)
