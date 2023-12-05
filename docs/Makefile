# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
# Do not fail the build if there are warnings
SPHINXOPTS    = -j8
SPHINXBUILD  ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Internal variables.
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(SPHINXOPTS) $(SOURCEDIR)
DOXYGEN = doxygen
ROOTDIR = ..

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile doxygen

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*

.PHONY: cpp-apidoc
cpp-apidoc:
	cd $(ROOTDIR)/cpp/apidoc && \
	$(DOXYGEN) Doxyfile

.PHONY: spark-apidoc
spark-apidoc:
	cd $(ROOTDIR)/spark && \
	mvn scala:doc

.PHONY: html
html: 
	python3 -m sphinx -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
