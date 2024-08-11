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

.PHONY: install_test
install_test:
		export JAVA_HOME=${JAVA_HOME_11_X64}
		cd ../maven-projects/spark && mvn --no-transfer-progress clean package -DskipTests -Dspotless.check.skip=true && cd ../../pyspark 
		export PYSPARK_HADOOP_VERSION=3.4
		poetry install --with=spark,tests

.PHONY: test
test:
		export JAVA_HOME=${JAVA_HOME_11_X64}
		export SPARK_TESTING=1
		poetry run pytest --cov

.PHONY: coverage_report
coverage_report:
		export JAVA_HOME=${JAVA_HOME_11_X64}
		export SPARK_TESTING=1
		poetry run pytest --cov --cov-report=html:coverage_html graphar_pyspark tests/

.PHONY: install_lint
install_lint:
		poetry install --with=lint

.PHONY: lint
lint:
		poetry run ruff check graphar_pyspark/

.PHONY: lint_fix
lint_fix:
		poetry run ruff check --fix graphar_pyspark/

.PHONY: install_docs
install_docs:
		poetry install --with=spark,docs

.PHONY: docs
docs:
		poetry run pdoc -t ./template --output-dir ./docs graphar_pyspark
