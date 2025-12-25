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

import json
import re
import sys
import urllib.request
from packaging.version import Version

PACKAGE_NAME = "graphar"
FILE_PATH = "python/pyproject.toml"

def get_next_version():
    versions = []
    urls = [
        f"https://pypi.org/pypi/{PACKAGE_NAME}/json",
        f"https://test.pypi.org/pypi/{PACKAGE_NAME}/json"
    ]
    
    print(f"Fetching versions for {PACKAGE_NAME}...")
    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                data = json.load(r)
                versions.extend(data.get("releases", {}).keys())
        except Exception:
            pass

    if not versions:
        return "0.0.1.dev1"

    latest = max([Version(v) for v in versions])
    print(f"Latest version found: {latest}")

    if latest.is_devrelease:
        return f"{latest.major}.{latest.minor}.{latest.micro}.dev{latest.dev + 1}"
    else:
        return f"{latest.major}.{latest.minor}.{latest.micro + 1}.dev1"

def main():
    new_ver = get_next_version()
    print(f"Target version: {new_ver}")

    try:
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            content = f.read()

        new_content, count = re.subn(
            r'(version\s*=\s*")([^"]+)(")', 
            rf'\g<1>{new_ver}\g<3>', 
            content
        )
        
        if count == 0:
            print(f"Error: Could not find 'version' key in {FILE_PATH}")
            sys.exit(1)

        with open(FILE_PATH, "w", encoding="utf-8") as f:
            f.write(new_content)
            
        print(f"Successfully updated {FILE_PATH} to {new_ver}")
        
    except FileNotFoundError:
        print(f"Error: File {FILE_PATH} not found.")
        sys.exit(1)

if __name__ == "__main__":
    main()
