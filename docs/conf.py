# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'GraphAr'
copyright = '2022, The GraphAr Authors'
author = 'The GraphAr Authors'

# The full version, including alpha/beta/rc tags
release = '0.1-alpha'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'breathe',
    'nbsphinx',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    "sphinx_copybutton",
    'sphinx_panels',
    'sphinxemoji.sphinxemoji',
    "sphinxext.opengraph",
]

# breathe
breathe_projects = {
    'GraphAr': os.path.abspath('../cpp/apidoc/xml'),
}
breathe_default_project = 'GraphAr'
breathe_debug_trace_directives = True
breathe_debug_trace_doxygen_ids = True
breathe_debug_trace_qualification = True

# jupyter notebooks
nbsphinx_execute = 'never'

source_suffix = ['.rst', '.md']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The root document.
root_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

html_theme_options = {
    "sidebar_hide_name": False,  # we use the logo
    "navigation_with_keys": True,
    "source_repository": "https://github.com/alibaba/GraphAr/",
    "source_branch": "main",
    "source_directory": "docs/",
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/alibaba/GraphAr",
            "html": "",
            "class": "fa fa-solid fa-github fa-2x",
        },
    ],
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = [
    'images/',
    "_static/",
]

html_css_files = [
    "css/brands.min.css",    # font-awesome
    "css/v4-shims.min.css",  # font-awesome
    "css/custom.css",
    "css/panels.css",
]

html_extra_path = [
    './.nojekyll',
]

html_title = 'GraphAr'
html_logo = "images/graphar-logo.png"
html_favicon = "images/graphar.ico"

html_show_copyright= True
html_show_sphinx = False
html_last_updated = True
