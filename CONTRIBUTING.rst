Contributing to GraphAr
========================

First off, thank you for considering contributing to GraphAr. It's people like you that make GraphAr better and better.

GraphAr is an open source project and we love to receive contributions from our community — you!
There are many ways to contribute, from improving the documentation, submitting bug reports and
feature requests or writing code which can be incorporated into GraphAr itself.

Reporting Bugs
-------------------

If you've noticed a bug in GraphAr, first make sure that you are testing against
the `latest version of GraphAr <https://github.com/alibaba/GraphAr/tree/main>`_ -
your issue may already have been fixed. If not, search our `issues list <https://github.com/alibaba/GraphAr/issues>`_
on GitHub in case a similar issue has already been opened.

If you get confirmation of your bug, `file an bug issue`_ before starting to code.

Requesting feature or enhancement
---------------------------------------

If you find yourself wishing for a feature that doesn't exist in GraphAr, you are probably not alone.
There are bound to be others out there with similar needs. Many of the features that GraphAr has today
have been added because our users saw the need. `Open an feature request issue`_ on GitHub
which describes the feature you would like to see, why you need it, and how it should work.

Support questions
-----------------
If you have a general question about GraphAr, please don't use the issue tracker for that.
The issue tracker is a tool to address bugs and feature requests in GraphAr itself.
Use one of the following resources for questions about using GraphAr or issues with your own code:

* Ask on our `Github Discussions`_

Contributing code and documentation changes
-------------------------------------------

If you would like to contribute a new feature or a bug fix to Elasticsearch,
please discuss your idea first on the GitHub issue. If there is no GitHub issue
for your idea, please open one. It may be that somebody is already working on
it, or that there are particular complexities that you should know about before
starting the implementation. There are often a number of ways to fix a problem
and it is important to find the right approach before spending time on a PR
that cannot be merged.

Fork & create a branch
^^^^^^^^^^^^^^^^^^^^^^^^

You will need to fork the main GraphAr code and clone it to your local machine. See
`github help page <https://help.github.com/articles/fork-a-repo>`_ for help.

Then you need to create a branch with a descriptive name.

A good branch name would be (where issue #42 is the ticket you're working on):

.. code:: shell

    git checkout -b 42-add-chinese-translations

Get the test suite running
^^^^^^^^^^^^^^^^^^^^^^^^^^

Make sure your system has the required dependencies for GraphAr. See the `GraphAr Dependencies`_ for more details.

Now initialize the submodules of GraphAr:

.. code:: shell

    git submodule update --init

Then you can do an out-of-source build using CMake and build the test suite:

.. code:: shell

    mkdir build
    cd build
    cmake .. -DBUILD_TESTS=ON
    make -j$(nproc)

Now you should be able to run the test suite:

.. code:: shell

    make test

Generate the document
^^^^^^^^^^^^^^^^^^^^^^

The documentation is generated using Doxygen and sphinx. You can build GraphAr's documentation in the :code:`docs/` directory using:

.. code:: shell

    make doc

The HTML documentation will be available under `docs/_build/html`.

Implement your fix or feature
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

At this point, you're ready to make your changes! Feel free to ask for help;
everyone is a beginner at first :smile_cat:

Get the code format & style right
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Your patch should follow the same conventions & pass the same code quality
checks as the rest of the project which follows the `Google C++ Style Guide <https://google.github.io/styleguide/cppguide.html>`_.

You can format your code by the CMakefile command:

.. code:: shell

    cd build
    make clformat

You can check & fix style issues by running the *cpplint* linter with the CMakefile command:

.. code:: shell

    cd build
    make cpplint

Submitting your changes
^^^^^^^^^^^^^^^^^^^^^^^

Once your changes and tests are ready to submit for review:

1. Test you changes

    Run the test suite to make sure that nothing is broken.

2. Sign the Contributor License Agreement(CLA)

    Please make sure you have signed our `Contributor License Agreement <https://www.alibaba.com/contributor-agreement/>`_.
We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction.
We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.

3. Submit a pull request

    At this point, you should switch back to your main branch and make sure it's
up to date with GraphAr's main branch:

.. code:: shell

    git remote add upstream https://github.com/alibaba/GraphAr.git
    git checkout main
    git pull upstream main

Then update your feature branch from your local copy of main, and push it!

.. code:: shell

    git checkout 42-add-chinese-translations
    git rebase main
    git push --set-upstream origin 42-add-chinese-translations

Finally, go to GitHub and `make a Pull Request`_ :D

Github Actions will run our test suite against different environments. We
care about quality, so your PR won't be merged until all tests pass.

Discussing and keeping your Pull Request updated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You will probably get feedback or requests for changes to your pull request.
This is a big part of the submission process so don't be discouraged!
It is a necessary part of the process in order to evaluate whether the changes
are correct and necessary.

If a maintainer asks you to "rebase" your PR, they're saying that a lot of code
has changed, and that you need to update your branch so it's easier to merge.

To learn more about rebasing in Git, there are a lot of `good <http://git-scm.com/book/en/Git-Branching-Rebasing>`_
`resources <https://help.github.com/en/github/using-git/about-git-rebase>`_ but here's the suggested workflow:

.. code:: shell

    git checkout 42-add-chinese-translations
    git pull --rebase upstream main
    git push --force-with-lease 42-add-chinese-translations

Feel free to post a comment in the pull request to ping reviewers if you are awaiting an answer
on something. If you encounter words or acronyms that seem unfamiliar, refer to this `glossary`_.

Merging a PR (maintainers only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A PR can only be merged into main by a maintainer if:

* It is passing CI.
* It has been approved by at least two maintainers. If it was a maintainer who
  opened the PR, only one extra approval is needed.
* It has no requested changes.
* It is up to date with current main.

Any maintainer is allowed to merge a PR if all of these conditions are
met.

Reviewing pull requests
-----------------------

All contributors who choose to review and provide feedback on Pull Requests have
a responsibility to both the project and the individual making the contribution.

When reviewing a pull request, the primary goals are for the codebase to improve
and for the person submitting the request to succeed. Even if a pull request does
not land, the submitters should come away from the experience feeling like their
effort was not wasted or unappreciated. Every pull request from a new contributor
is an opportunity to grow the community.

.. _file an bug issue: https://github.com/alibaba/GraphAr/issues/new/choose

.. _Open an feature request issue: https://github.com/alibaba/GraphAr/issues/new/choose

.. _fork GraphAr: https://help.github.com/articles/fork-a-repo

.. _make a Pull Request: https://help.github.com/articles/creating-a-pull-request

.. _Github Discussions: https://github.com/alibaba/GraphAr/discussions

.. _git rebasing: http://git-scm.com/book/en/Git-Branching-Rebasing

.. _interactive rebase: https://help.github.com/en/github/using-git/about-git-rebase

.. _GraphAr Dependencies: https://github.com/alibaba/GraphAr#dependencies

.. _glossary: https://chromium.googlesource.com/chromiumos/docs/+/HEAD/glossary.md
