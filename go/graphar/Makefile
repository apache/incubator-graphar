# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Coverage threshold per package (percent). CI fails below this floor.
COVERAGE_MIN ?= 85

GO        ?= go
GOFMT     ?= gofmt
GOIMPORTS ?= goimports
COVER_OUT ?= coverage.out

# Local import prefix for goimports grouping (see .golangci.yml).
LOCAL_PREFIX := github.com/apache/incubator-graphar/go/graphar

# License header check: the same tool the CI job uses (skywalking-eyes),
# scoped to our Go files. REPO_ROOT is needed because the tool enumerates from
# the git root.
LICENSE_EYE ?= ghcr.io/apache/skywalking-eyes/license-eye:latest
REPO_ROOT   := $(shell git rev-parse --show-toplevel 2>/dev/null || echo $(abspath $(CURDIR)/../..))

.PHONY: help all fmt fmt-check vet lint test test-race coverage coverage-check tidy clean ci license-check

help:
	@echo "Targets:"
	@echo "  fmt             - run gofmt -w and goimports on all .go files"
	@echo "  fmt-check       - fail if any file needs gofmt"
	@echo "  vet             - run go vet ./..."
	@echo "  lint            - run golangci-lint (must be installed)"
	@echo "  test            - run go test ./... with race detector off"
	@echo "  test-race       - run go test -race ./..."
	@echo "  coverage        - produce $(COVER_OUT) with -race and print per-package coverage"
	@echo "  coverage-check  - fail if total coverage < COVERAGE_MIN ($(COVERAGE_MIN)%)"
	@echo "  tidy            - go mod tidy"
	@echo "  license-check   - check ASF headers on our Go files (skywalking-eyes, docker)"
	@echo "  ci              - fmt-check + vet + lint + coverage + coverage-check"

all: ci

fmt:
	$(GOFMT) -w .
	@if command -v $(GOIMPORTS) >/dev/null 2>&1; then \
	  $(GOIMPORTS) -w -local $(LOCAL_PREFIX) .; \
	else \
	  echo "$(GOIMPORTS) not installed; skipping import grouping"; \
	  echo "  install: go install golang.org/x/tools/cmd/goimports@latest"; \
	fi

fmt-check:
	@diff=$$($(GOFMT) -l .); \
	if [ -n "$$diff" ]; then \
	  echo "gofmt needs to be run on the following files:"; \
	  echo "$$diff"; \
	  exit 1; \
	fi

vet:
	$(GO) vet ./...

# golangci-lint is optional locally; CI installs it explicitly.
# Use --version probe rather than command -v: asdf shims pretend to exist
# even when no version is selected, so command -v can give a false positive.
lint:
	@if golangci-lint --version >/dev/null 2>&1; then \
	  golangci-lint run ./...; \
	else \
	  echo "golangci-lint not available locally; skipping (CI will run it)"; \
	fi

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

coverage:
	$(GO) test -race -coverprofile=$(COVER_OUT) -covermode=atomic ./...
	@$(GO) tool cover -func=$(COVER_OUT) | tail -n 1

# coverage-check enforces the COVERAGE_MIN floor on an existing profile. It does
# not depend on `coverage`: CI (and `make ci`) already produce a -race profile,
# and regenerating here would both duplicate that run and overwrite the -race
# profile with a plain one. Only a bare `make coverage-check` with no profile on
# disk builds one first.
# When the profile has no executable statements yet (M0 bootstrap, before any
# package ships testable code), the floor is skipped, a floor is for guarding
# against regressions, not for blocking an empty repo. Statement count comes
# from non-mode/non-summary lines in the profile file itself.
coverage-check:
	@test -f $(COVER_OUT) || $(MAKE) coverage
	@stmts=$$(grep -cv -E '^(mode:|$$)' $(COVER_OUT) || true); \
	if [ "$$stmts" -eq 0 ]; then \
	  echo "no executable statements yet; skipping coverage floor"; \
	  exit 0; \
	fi; \
	total=$$($(GO) tool cover -func=$(COVER_OUT) | tail -n 1 | awk '{print $$3}' | tr -d '%'); \
	awk -v t=$$total -v m=$(COVERAGE_MIN) 'BEGIN { if (t+0 < m+0) { printf "coverage %.1f%% below minimum %s%%\n", t, m; exit 1 } else { printf "coverage %.1f%% >= minimum %s%%\n", t, m } }'

tidy:
	$(GO) mod tidy

# license-check runs the CI license tool (skywalking-eyes) over our Go files.
# The authoritative whole-repo gate is the CI job (config .github/.licenserc.yaml);
# this checks the files we own, using the same tool so results agree.
license-check:
	@command -v docker >/dev/null 2>&1 || { echo "docker not found; needed for license-check"; exit 1; }
	@cfg=$$(mktemp) && trap 'rm -f "$$cfg"' EXIT; \
	  printf 'header:\n  license:\n    spdx-id: Apache-2.0\n    copyright-owner: Apache Software Foundation\n  paths:\n    - "go/graphar/**/*.go"\n' > "$$cfg"; \
	  docker run --rm -v "$(REPO_ROOT):/w" -w /w -v "$$cfg:/lc.yaml" \
	    $(LICENSE_EYE) -c /lc.yaml header check

clean:
	rm -f $(COVER_OUT)

ci: fmt-check vet lint coverage coverage-check
