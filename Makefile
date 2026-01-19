# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= uv run sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

.PHONY: install
install: ## Install the virtual environment and install the pre-commit hooks
	@echo "Creating virtual environment using uv"
	@uv sync --dev
	@uv run prek install

.PHONY: check
check: ## Run code quality tools.
	@echo "Linting code: Running pre-commit via prek"
	@uv run prek run -a

.PHONY: test
test: ## Test the code with pytest
	@echo "Testing code: Running pytest"
	@uv run python -m pytest $(TEST_FILTER)

.PHONY: compileall
compileall: ## Byte-compile Python sources.
	@uv run python -m compileall pyobvector tests

.PHONY: build
build: ## Build wheel file
	@echo "Creating wheel file"
	@uv build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help install check test build compileall Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
