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
  mvn --no-transfer-progress clean package -DskipTests -Dspotless.check.skip=true && \
	mvn --no-transfer-progress scala:doc

.PHONY: html
html: cpp-apidoc spark-apidoc
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	rm -fr $(BUILDDIR)/html/spark/reference
	cp -fr $(ROOTDIR)/spark/target/site/scaladocs $(BUILDDIR)/html/spark/reference/
	cd $(ROOTDIR)/java && \
	mvn --no-transfer-progress -P javadoc javadoc:aggregate \
				-Dmaven.antrun.skip=true \
				-DskipTests \
				-Djavadoc.output.directory=$(ROOTDIR)/docs/$(BUILDDIR)/html/java/ \
				-Djavadoc.output.destDir=reference \
				--quiet
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: pyspark-apidoc
pyspark-apidoc:
	cd $(ROOTDIR)/pyspark && \
	poetry run sphinx-apidoc -o $(ROOTDIR)/docs/pyspark/api graphar_pyspark/

.PHONY: html-poetry
html-poetry:
	cd $(ROOTDIR)/pyspark && \
	poetry run bash -c "cd $(ROOTDIR)/docs && $(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html"
	rm -fr $(BUILDDIR)/html/spark/reference
	cp -fr $(ROOTDIR)/spark/target/site/scaladocs $(BUILDDIR)/html/spark/reference/
	cd $(ROOTDIR)/java && \
	mvn --no-transfer-progress -P javadoc javadoc:aggregate \
				-Dmaven.antrun.skip=true \
				-DskipTests \
				-Djavadoc.output.directory=$(ROOTDIR)/docs/$(BUILDDIR)/html/java/ \
				-Djavadoc.output.destDir=reference \
				--quiet
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

