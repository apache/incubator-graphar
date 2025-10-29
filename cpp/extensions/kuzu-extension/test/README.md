## Test cases/examples

```shell
# test for copy clause
g++ graphar_extension_copy_test.cpp -std=c++20 -ggdb -lkuzu -lpthread -o graphar_extension_copy_test
./graphar_extension_copy_test

# test for load clause
g++ graphar_extension_load_test.cpp -std=c++20 -ggdb -lkuzu -lpthread -o graphar_extension_load_test
./graphar_extension_load_test

# test for copy to clause
g++ graphar_extension_copy_to_test.cpp -std=c++20 -ggdb -lkuzu -lpthread -o graphar_extension_copy_to_test
./graphar_extension_copy_to_test

# test for metadata
g++ graphar_extension_metadata_test.cpp -std=c++20 -ggdb -lkuzu -lpthread -o graphar_extension_metadata_test
./graphar_extension_metadata_test
```