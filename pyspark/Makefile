# copyright 2022-2023 alibaba group holding limited.
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#     http://www.apache.org/licenses/license-2.0
#
# unless required by applicable law or agreed to in writing, software
# distributed under the license is distributed on an "as is" basis,
# without warranties or conditions of any kind, either express or implied.
# see the license for the specific language governing permissions and
# limitations under the license.

.PHONY: install_test
install_test:
		export JAVA_HOME=${JAVA_HOME_11_X64}
		cd ../spark && mvn --no-transfer-progress clean package -DskipTests -Dspotless.check.skip=true && cd ../pyspark
		export PYSPARK_HADOOP_VERSION=3.2
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
