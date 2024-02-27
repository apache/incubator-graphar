# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
# Do not fail the build if there are warnings
SPHINXOPTS    = -j8
SPHINXBUILD  ?= sphinx.cmd.build
SOURCEDIR     = .
BUILDDIR      = _build

# Internal variables.
ALLSPHINXOPTS   = -b html -d ../docs/$(BUILDDIR)/doctrees $(SPHINXOPTS)
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
  mvn --no-transfer-progress clean install -DskipTests -Dspotless.check.skip=true && \
	mvn --no-transfer-progress scala:doc

.PHONY: pyspark-apidoc
pyspark-apidoc:
	cd ../pyspark && poetry run sphinx-apidoc -o ../docs/pyspark/api graphar_pyspark/

.PHONY: html-poetry
html-poetry: cpp-apidoc spark-apidoc pyspark-apidoc
	echo "Generate docs..."
	cd $(ROOTDIR)/pyspark && poetry run python -m $(SPHINXBUILD) $(ALLSPHINXOPTS) $(ROOTDIR)/docs $(ROOTDIR)/docs/$(BUILDDIR)/html
	rm -fr $(BUILDDIR)/html/spark/reference
	cp -fr $(ROOTDIR)/spark/graphar/target/site/scaladocs $(BUILDDIR)/html/spark/reference/
	cd $(ROOTDIR)/java && \
	mvn --no-transfer-progress -P javadoc javadoc:aggregate \
				-Dmaven.antrun.skip=true \
				-DskipTests \
				-Djavadoc.output.directory=$(ROOTDIR)/docs/$(BUILDDIR)/html/java/ \
				-Djavadoc.output.destDir=reference \
				--quiet
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

