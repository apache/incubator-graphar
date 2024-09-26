# GraphAr Cli (Under Development)

GraphAr Cli uses [pybind11][] and [scikit-build-core][] to bind C++ code into Python and build command line tools through Python. Command line tools developed using [typer][].

[pybind11]: https://pybind11.readthedocs.io
[scikit-build-core]: https://scikit-build-core.readthedocs.io
[typer]: https://typer.tiangolo.com/

## Requirements

- Ubuntu 22.04
- Cmake >= 3.15
- Arrow >= 12.0
- Python >= 3.7
- pip == latest


The best testing environment is `ghcr.io/apache/graphar-dev` Docker environment.

And using Python in conda or venv is a good choice. 

## Installation

- Clone this repository
- `pip install ./cli` or set verbose level `pip install -v ./cli`

## Usage

```bash
graphar-cli --help
```


