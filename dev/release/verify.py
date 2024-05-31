#!/usr/bin/env python3
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

# Derived from Apache OpenDAL v0.46.0
# https://github.com/apache/opendal/blob/84586e5/scripts/verify.py

import subprocess
import os
from pathlib import Path

BASE_DIR = Path(os.getcwd())

# Define colors for output
YELLOW = "\033[37;1m"
GREEN = "\033[32;1m"
ENDCOLOR = "\033[0m"


def check_signature(pkg):
    """Check the GPG signature of the package."""
    try:
        subprocess.check_call(["gpg", "--verify", f"{pkg}.asc", pkg])
        print(f"{GREEN}> Success to verify the gpg sign for {pkg}{ENDCOLOR}")
    except subprocess.CalledProcessError:
        print(f"{YELLOW}> Failed to verify the gpg sign for {pkg}{ENDCOLOR}")


def check_sha512sum(pkg):
    """Check the sha512 checksum of the package."""
    try:
        subprocess.check_call(["sha512sum", "--check", f"{pkg}.sha512"])
        print(f"{GREEN}> Success to verify the checksum for {pkg}{ENDCOLOR}")
    except subprocess.CalledProcessError:
        print(f"{YELLOW}> Failed to verify the checksum for {pkg}{ENDCOLOR}")


def extract_packages():
    for file in BASE_DIR.glob("*.tar.gz"):
        subprocess.run(["tar", "-xzf", file], check=True)


def check_license(dir):
    print(f"> Start checking LICENSE file in {dir}")
    if not (dir / "LICENSE").exists():
        raise f"{YELLOW}> LICENSE file is not found{ENDCOLOR}"
    print(f"{GREEN}> LICENSE file exists in {dir}{ENDCOLOR}")


def check_notice(dir):
    print(f"> Start checking NOTICE file in {dir}")
    if not (dir / "NOTICE").exists():
        raise f"{YELLOW}> NOTICE file is not found{ENDCOLOR}"
    print(f"{GREEN}> NOTICE file exists in {dir}{ENDCOLOR}")


def install_conda():
    print("Start installing conda")
    subprocess.run(["wget", "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"], check=True)
    subprocess.run(["bash", "Miniconda3-latest-Linux-x86_64.sh", "-b"], check=True)
    print(f"{GREEN}Success to install conda{ENDCOLOR}")


def maybe_setup_conda(dependencies):
    # Optionally setup a conda environment with the given dependencies
    if ("USE_CONDA" in os.environ) and (os.environ["USE_CONDA"] > 0):
        print("Configuring conda environment...")
        subprocess.run(["conda", "deactivate"], check=False, stderr=subprocess.STDOUT)
        create_env_command = ["conda", "create", "--name", "graphar", "--yes", "python=3.8"]
        subprocess.run(create_env_command, check=True, stderr=subprocess.STDOUT)
        install_deps_command = ["conda", "install", "--name", "graphar", "--yes"] + dependencies
        subprocess.run(install_deps_command, check=True, stderr=subprocess.STDOUT)
        subprocess.run(["conda", "activate", "graphar"], check=True, stderr=subprocess.STDOUT, shell=True)


def build_and_test_cpp(dir):
    print("Start building, install and test C++ library")

    maybe_setup_conda(["--file", f"{dir}/dev/release/conda_env_cpp.txt"])

    cmake_command = ["cmake", ".", "-DBUILD_TEST=ON", "-DBUILD_EXAMPLES=ON", "-DBUILD_BENCHMARKS=ON"]
    subprocess.run(
        cmake_command,
        cwd=dir / "cpp",
        check=True,
        stderr=subprocess.STDOUT,
    )
    build_and_install_command = [
        "cmake",
        "--build",
        ".",
        "--target",
        "install",
    ]
    subprocess.run(
        build_and_install_command,
        cwd=dir / "cpp",
        check=True,
        stderr=subprocess.STDOUT,
    )
    test_command = [
        "ctest",
        "--output-on-failure",
        "--timeout",
        "300",
        "-VV"
    ]
    subprocess.run(
        test_command,
        cwd=dir / "cpp",
        check=True,
        stderr=subprocess.STDOUT,
    )
    print(f"{GREEN}Success to build graphar c++{ENDCOLOR}")


def build_and_test_scala(dir):
    print("Start building, install and test Scala with Spark library")

    maybe_setup_conda(["--file", f"{dir}/dev/release/conda_env_scala.txt"])

    build_command_32=["mvn", "clean", "package", "-P", "datasource32"]
    subprocess.run(
        build_command_32,
        cwd=dir / "maven-projects/spark",
        check=True,
        stderr=subprocess.STDOUT,
    )
    build_command_33=["mvn", "clean", "package", "-P", "datasource33"]
    subprocess.run(
        build_command_33,
        cwd=dir / "maven-projects/spark",
        check=True,
        stderr=subprocess.STDOUT,
    )

    print(f"{GREEN}Success to build graphar scala{ENDCOLOR}")

if __name__ == "__main__":
    # Get a list of all files in the current directory
    files = [f for f in os.listdir(".") if os.path.isfile(f)]

    for pkg in files:
        # Skip files that don't have a corresponding .asc or .sha512 file
        if not os.path.exists(f"{pkg}.asc") or not os.path.exists(f"{pkg}.sha512"):
            continue

        print(f"> Checking {pkg}")

        # Perform the checks
        check_signature(pkg)
        check_sha512sum(pkg)

    extract_packages()

    for dir in BASE_DIR.glob("apache-graphar-*-src/"):
        check_license(dir)
        check_notice(dir)
        build_and_test_cpp(dir)
        build_and_test_scala(dir)
