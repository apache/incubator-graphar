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
# https://github.com/apache/opendal/blob/84586e5/scripts/release.py

import subprocess
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

def archive_source_package():
    print(f"Archive source package started")

    # TODO: get the package version by get_version
    # version = get_package_version(path)
    version = "0.15.0"
    name = f"apache-graphar-{version}-incubating-src"

    archive_command = [
        "git",
        "archive",
        "--prefix",
        f"apache-graphar-incubating-{version}-src/",
        "-o",
        f"{ROOT_DIR}/dist/{name}.tar.gz",
        "HEAD",
    ]
    subprocess.run(
        archive_command,
        cwd=ROOT_DIR,
        check=True,
    )

    print(f"Archive source package to dist/{name}.tar.gz")


def generate_signature():
    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Generate signature for {i}")
        subprocess.run(
            ["gpg", "--yes", "--armor", "--output", f"{i}.asc", "--detach-sig", str(i)],
            cwd=ROOT_DIR / "dist",
            check=True,
        )

    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Check signature for {i}")
        subprocess.run(
            ["gpg", "--verify", f"{i}.asc", str(i)], cwd=ROOT_DIR / "dist", check=True
        )


def generate_checksum():
    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Generate checksum for {i}")
        subprocess.run(
            ["sha512sum", str(i.relative_to(ROOT_DIR / "dist"))],
            stdout=open(f"{i}.sha512", "w"),
            cwd=ROOT_DIR / "dist",
            check=True,
        )

    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Check checksum for {i}")
        subprocess.run(
            ["sha512sum", "--check", f"{str(i.relative_to(ROOT_DIR / 'dist'))}.sha512"],
            cwd=ROOT_DIR / "dist",
            check=True,
        )


if __name__ == "__main__":
    (ROOT_DIR / "dist").mkdir(exist_ok=True)
    archive_source_package()
    generate_signature()
    generate_checksum()
