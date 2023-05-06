/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

extern "C" {
#include "grin/include/topology/datatype.h"
}
#include "grin/src/predefine.h"

int grin_get_int32(const void* ptr) {
  return *static_cast<const int32_t*>(ptr);
}

unsigned int grin_get_uint32(const void* ptr) {
  return *static_cast<const uint32_t*>(ptr);
}

long long int grin_get_int64(const void* ptr) {
  return *static_cast<const int64_t*>(ptr);
}

unsigned long long int grin_get_uint64(const void* ptr) {
  return *static_cast<const uint64_t*>(ptr);
}

float grin_get_float(const void* ptr) {
  return *static_cast<const float*>(ptr);
}

double grin_get_double(const void* ptr) {
  return *static_cast<const double*>(ptr);
}

const char* grin_get_string(const void* ptr) {
  const char* s = static_cast<const char*>(ptr);
  int len = strlen(s) + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", s);
  return out;
}

const void* grin_put_int32(int value) { return new int32_t(value); }

const void* grin_put_uint32(unsigned int value) { return new uint32_t(value); }

const void* grin_put_int64(long long int value) { return new int64_t(value); }

const void* grin_put_uint64(unsigned long long int value) {
  return new uint64_t(value);
}

const void* grin_put_float(float value) { return new float(value); }

const void* grin_put_double(double value) { return new double(value); }

const void* grin_put_string(const char* value) {
  int len = strlen(value) + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", value);
  return out;
}
