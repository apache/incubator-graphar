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

REGISTRY ?= docker.io/apache
REPO ?= graphar-dev
VERSION ?= latest

.PHONY: docker-build
docker-build:
	docker build . -t $(REGISTRY)/$(REPO):$(VERSION) -t $(REGISTRY)/$(REPO):latest

.PHONY: docker-push
docker-push:
	@{\
		docker buildx create --use --driver docker-container --name graphar-dev ;\
		docker buildx build --push --platform linux/amd64,linux/arm64 -t $(REGISTRY)/$(REPO):$(VERSION) -t $(REGISTRY)/$(REPO):latest . ;\
		build_exit_code=$$? ;\
		docker buildx rm graphar-dev ;\
		exit $$build_exit_code;\
	}

docker-release: docker-build docker-push
