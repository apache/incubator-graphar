Contributing to GraphAr
========================

First off, thank you for considering contributing to GraphAr. It's people like you that make GraphAr better and better.

GraphAr is an open source project and we love to receive contributions from our community — you!
There are many ways to contribute, from improving the documentation, submitting bug reports and
feature requests or writing code which can be incorporated into GraphAr itself.

Note that no matter how you contribute, your participation is governed by our `Code of Conduct`_.

Reporting bug
-------------------

If you've noticed a bug in GraphAr, first make sure that you are testing against
the `latest version of GraphAr <https://github.com/alibaba/GraphAr/tree/main>`_ -
your issue may already have been fixed. If not, search our `issues list <https://github.com/alibaba/GraphAr/issues>`_
on GitHub in case a similar issue has already been opened.

If you get confirmation of your bug, `file a bug issue`_ before starting to code.

Requesting feature or enhancement
---------------------------------------

If you find yourself wishing for a feature that doesn't exist in GraphAr, you are probably not alone.
There are bound to be others out there with similar needs. Many of the features that GraphAr has today
have been added because our users saw the need.

`Open a feature request issue`_ on GitHub which describes the feature you would
like to see, why you need it, and how it should work.

Support questions
-----------------
If you have a general question about GraphAr, please don't use the issue tracker for that.
The issue tracker is a tool to address bugs and feature requests in GraphAr itself.
Use our `Github Discussions`_ for questions about using GraphAr or issues with your own code:


Contributing code and documentation changes
-------------------------------------------

If you would like to contribute a new feature or a bug fix to GraphAr,
please discuss your idea first on the GitHub issue. If there is no GitHub issue
for your idea, please open one. It may be that somebody is already working on
it, or that there are particular complexities that you should know about before
starting the implementation. There are often a number of ways to fix a problem
and it is important to find the right approach before spending time on a PR
that cannot be merged.

Minor Fixes
^^^^^^^^^^^^

Any functionality change should have a GitHub issue opened. For minor changes that
affect documentation, you do not need to open up a GitHub issue. Instead you can
prefix the title of your PR with "[MINOR] " if meets the following guidelines:

*  Grammar, usage and spelling fixes that affect no more than 2 files
*  Documentation updates affecting no more than 2 files and not more
   than 500 words.

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

How to generate the document
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to improve the document, you need to know how to generate the docs.

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

You can format your code by the command:

.. code:: shell

    make clformat

You can check & fix style issues by running the *cpplint* linter with the command:

.. code:: shell

    make cpplint

Submitting your changes
^^^^^^^^^^^^^^^^^^^^^^^

Once your changes and tests are ready to submit for review:

1. Test you changes

Run the test suite to make sure that nothing is broken.

2. Sign the Contributor License Agreement (CLA)

Please make sure you have signed our `Contributor License Agreement`_.
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
`resources <https://help.github.com/en/github/using-git/about-git-rebase>`_, but here's the suggested workflow:

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

Shipping a release (maintainers only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Maintainers need to do the following to push out a release:

1. Switch to the main branch and make sure it's up to date.

.. code:: shell

    git checkout main
    git pull upstream main

2. Tag the release with a version number and push it to GitHub. Note that the version number should follow `semantic versioning <https://semver.org/#summary>`_. e.g.: v0.1.0.

.. code:: shell

    git tag -a v0.1.0 -m "GraphAr v0.1.0"
    git push upstream v0.1.0

3. The release draft will be automatically built to GitHub by GitHub Actions. You can edit the release notes draft on `GitHub <https://github.com/alibaba/GraphAr/releases>`_ to add more details.
4. Publish the release.

.. the reviewing part document is referred and derived from
.. https://github.com/nodejs/node/blob/main/doc/contributing/pull-requests.md#the-process-of-making-changes
Reviewing pull requests
-----------------------

All contributors who choose to review and provide feedback on Pull Requests have
a responsibility to both the project and the individual making the contribution.
Reviews and feedback must be helpful, insightful, and geared towards improving
the contribution as opposed to simply blocking it. Do not expect to be able to
block a pull request from advancing simply because you say "No" without giving
an explanation. Be open to having your mind changed. Be open to working with the
contributor to make the pull request better.

Reviews that are dismissive or disrespectful of the contributor or any other
reviewers are strictly counter to the `Code of Conduct`_ and will not be tolerated.

When reviewing a pull request, the primary goals are for the codebase to improve
and for the person submitting the request to succeed. Even if a pull request does
not land, the submitters should come away from the experience feeling like their
effort was not wasted or unappreciated. Every pull request from a new contributor
is an opportunity to grow the community.

Review a bit at a time
^^^^^^^^^^^^^^^^^^^^^^^

Do not overwhelm new contributors.

It is tempting to micro-optimize and make everything about relative performance,
perfect grammar, or exact style matches. Do not succumb to that temptation.

Focus first on the most significant aspects of the change:

1. Does this change make sense for GraphAr?
2. Does this change make GraphAr better, even if only incrementally?
3. Are there clear bugs or larger scale issues that need attending to?
4. Is the commit message readable and correct? If it contains a breaking change
   is it clear enough?

When changes are necessary, *request* them, do not *demand* them, and do not
assume that the submitter already knows how to add a test or run a benchmark.

Specific performance optimization techniques, coding styles, and conventions
change over time. The first impression you give to a new contributor never does.

Nits (requests for small changes that are not essential) are fine, but try to
avoid stalling the pull request. Most nits can typically be fixed by the
GraphAr collaborator landing the pull request but they can also be an
opportunity for the contributor to learn a bit more about the project.

It is always good to clearly indicate nits when you comment: e.g.
:code:`Nit: change foo() to bar(). But this is not blocking.`

If your comments were addressed but were not folded automatically after new
commits or if they proved to be mistaken, please, `hide them <https://docs.github.com/en/communities/moderating-comments-and-conversations/managing-disruptive-comments#hiding-a-comment>`_
with the appropriate reason to keep the conversation flow concise and relevant.

Be aware of the person behind the code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Be aware that *how* you communicate requests and reviews in your feedback can
have a significant impact on the success of the pull request. Yes, we may land
a particular change that makes GraphAr better, but the individual might just
not want to have anything to do with GraphAr ever again. The goal is not just
having good code.

Respect the minimum wait time for comments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a minimum waiting time which we try to respect for non-trivial
changes, so that people who may have important input in such a distributed
project are able to respond.

For non-trivial changes, pull requests must be left open for at least 48 hours.
Sometimes changes take far longer to review, or need more specialized review
from subject-matter experts. When in doubt, do not rush.

Trivial changes, typically limited to small formatting changes or fixes to
documentation, may be landed within the minimum 48 hour window.

Abandoned or stalled pull requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a pull request appears to be abandoned or stalled, it is polite to first
check with the contributor to see if they intend to continue the work before
checking if they would mind if you took it over (especially if it just has
nits left). When doing so, it is courteous to give the original contributor
credit for the work they started (either by preserving their name and email
address) in the commit log, or by using an :code:`Author:` meta-data tag in the
commit.

Approving a change
^^^^^^^^^^^^^^^^^^^

Any GraphAr core collaborator (any GitHub user with commit rights in the
:code:`alibaba/GraphAr` repository) is authorized to approve any other contributor's
work. Collaborators are not permitted to approve their own pull requests.

Collaborators indicate that they have reviewed and approve of the changes in
a pull request either by using GitHub's Approval Workflow, which is preferred,
or by leaving an :code:`LGTM` ("Looks Good To Me") comment.

When explicitly using the "Changes requested" component of the GitHub Approval
Workflow, show empathy. That is, do not be rude or abrupt with your feedback
and offer concrete suggestions for improvement, if possible. If you're not
sure **how** a particular change can be improved, say so.

Most importantly, after leaving such requests, it is courteous to make yourself
available later to check whether your comments have been addressed.

If you see that requested changes have been made, you can clear another
collaborator's :code:`Changes requested` review.

Change requests that are vague, dismissive, or unconstructive may also be
dismissed if requests for greater clarification go unanswered within a
reasonable period of time.

Use :code:`Changes requested` to block a pull request from landing. When doing so,
explain why you believe the pull request should not land along with an
explanation of what may be an acceptable alternative course, if any.

Performance is not everything
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

GraphAr has always optimized for speed of execution. If a particular change
can be shown to make some part of GraphAr faster, it's quite likely to be
accepted.

That said, performance is not the only factor to consider. GraphAr also
optimizes in favor of not breaking existing code in the ecosystem, and not
changing working functional code just for the sake of changing.

If a particular pull request introduces a performance or functional
regression, rather than simply rejecting the pull request, take the time to
work *with* the contributor on improving the change. Offer feedback and
advice on what would make the pull request acceptable, and do not assume that
the contributor should already know how to do that. Be explicit in your
feedback.

Continuous integration testing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All pull requests that contain changes to code must be run through
continuous integration (CI) testing at `Github Actions <https://github.com/alibaba/GraphAr/actions>`_.

The pull request change will trigger a CI testing run. Ideally, the code change
will pass ("be green") on all platform configurations supported by GraphAr.
This means that all tests pass and there are no linting errors. In reality,
however, it is not uncommon for the CI infrastructure itself to fail on specific
platforms ("be red"). It is vital to visually inspect the results of all failed ("red") tests
to determine whether the failure was caused by the changes in the pull request.

.. _Code of Conduct: https://github.com/alibaba/GraphAr/blob/main/CODE_OF_CONDUCT.md

.. _file a bug issue: https://github.com/alibaba/GraphAr/issues/new?assignees=&labels=Bug&template=bug_report.yml&title=%5BBug%5D%3A+%3Ctitle%3E

.. _Open a feature request issue: https://github.com/alibaba/GraphAr/issues/new?assignees=&labels=enhancement&template=feature_request.md&title=%5BFeat%5D

.. _fork GraphAr: https://help.github.com/articles/fork-a-repo

.. _make a Pull Request: https://help.github.com/articles/creating-a-pull-request

.. _Github Discussions: https://github.com/alibaba/GraphAr/discussions

.. _git rebasing: http://git-scm.com/book/en/Git-Branching-Rebasing

.. _interactive rebase: https://help.github.com/en/github/using-git/about-git-rebase

.. _GraphAr Dependencies: https://github.com/alibaba/GraphAr#dependencies

.. _Contributor License Agreement: https://cla-assistant.io/alibaba/GraphAr

.. _glossary: https://chromium.googlesource.com/chromiumos/docs/+/HEAD/glossary.md
