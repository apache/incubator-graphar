# GraphAr Cli Tool

This is a project that depends on the GraphAr spark library.

## Building

To build this project, we need to use `Maven` to enable the
`datasource` and `graphar-cli` profiles in the `spark`
directory (that is, the parent directory of the current path).

```bash
    $ git clone https://github.com/apache/incubator-graphar.git
    $ cd incubator-graphar
    $ cd mavens-projects/spark
```


Build the package:

```bash
    $ mvn clean install -DskipTests -P datasources-32,graphar-cli
```

## Running

The build produces a shaded Jar that can be run using the `spark-submit` command:

```bash
    $ cd graphar-cli/target
    $ spark-submit --class org.apache.graphar.cli.Main graphar-cli-0.12.0-SNAPSHOT-shaded.jar
```

For a shorter command-line invocation, add an alias to your shell like this:

```
alias graphar="spark-submit --class org.apache.graphar.cli.Main /path/to/graphar-cli-0.12.0-SNAPSHOT-shaded.jar"
```
