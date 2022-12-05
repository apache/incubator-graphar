Contributing to GraphAr
========================

First off, thank you for considering contributing to GraphAr. It's people like you that make GraphAr better and better.

GraphAr is an open source project and we love to receive contributions from our community — you!
There are many ways to contribute, from improving the documentation, submitting bug reports and
feature requests or writing code which can be incorporated into GraphAr itself.

Following these guidelines helps to communicate that you respect the time of the developers managing
and developing this open source project. In return, they should reciprocate that respect in addressing
your issue, assessing changes, and helping you finalize your pull requests.


Getting Started
----------------

Where do I go from here?
^^^^^^^^^^^^^^^^^^^^^^^^

If you've noticed a bug or have a feature request, `make one<new issue>`_! It's
generally best if you get confirmation of your bug or approval for your feature
request this way before starting to code.

If you have a general question about GraphAr, you can post it on [Discussions]_,
the issue tracker is only for bugs and feature requests.

Fork & create a branch
^^^^^^^^^^^^^^^^^^^^^^^^

If this is something you think you can fix, then `fork GraphAr`_ and create
a branch with a descriptive name.

A good branch name would be (where issue #325 is the ticket you're working on):

.. code:: shell

    git checkout -b 325-add-chinese-translations

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

You can check & fix style issues by running the `cpplint` linter with the CMakefile command:

.. code:: shell

    cd build
    make cpplint

Make a Pull Request
^^^^^^^^^^^^^^^^^^^^

At this point, you should switch back to your main branch and make sure it's
up to date with GraphAr's main branch:

.. code:: shell

    git remote add upstream https://github.com/alibaba/GraphAr.git
    git checkout main
    git pull upstream main

Then update your feature branch from your local copy of main, and push it!

.. code:: shell

    git checkout 325-add-chinese-translations
    git rebase main
    git push --set-upstream origin 325-add-chinese-translations

Finally, go to GitHub and `make a Pull Request`_ :D

Github Actions will run our test suite against different environments. We
care about quality, so your PR won't be merged until all tests pass.

Keeping your Pull Request updated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a maintainer asks you to "rebase" your PR, they're saying that a lot of code
has changed, and that you need to update your branch so it's easier to merge.

To learn more about rebasing in Git, there are a lot of `good<git rebasing>_`
`resources<interactive rebase>`_ but here's the suggested workflow:

.. code:: shell

    git checkout 325-add-chinese-translations
    git pull --rebase upstream main
    git push --force-with-lease 325-add-chinese-translations

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

.. _new issue: https://github.com/alibaba/GraphAr/issues/new/choose

.. _folk GraphAr: https://help.github.com/articles/fork-a-repo

.. _make a Pull Request: https://help.github.com/articles/creating-a-pull-request

.. _git rebasing: http://git-scm.com/book/en/Git-Branching-Rebasing

.. _interactive rebase: https://help.github.com/en/github/using-git/about-git-rebase

.. _GraphAr Dependencies: https://github.com/alibaba/GraphAr#dependencies
