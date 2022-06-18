# SPEck: Mining Statistically-significant Sequential Patterns Efficiently with Exact Sampling

This code is derived from the one for
[ProMiSe](https://github.com/vandinlab/promise) by Andrea Tonon and Fabio
Vandin. As such, it is distributed under the [GNU General Public License, Version
3](LICENSE) or later.

## Software requirements

A recent Java SDK, [Maven](https://maven.apache.org/),
[R](https://www.r-project.org/), [Python3](https://www.python.org/), and R
packages `ggplot2` and `tidyverse`.

## Building process

1. Enter the `SPECK` directory
```sh
cd SPECK
```

2. Create the jar
```sh
mvn clean package
```

## Running a generic experiment

1. Create a configuration file in the following format with all variables as
   strings:

```
{
    "procs": <NUMBER_OF_PROCESSORS>,
    "reps": <NUMBER_OF_REPETITIONS>,
    "P": [<P_1>, <P_2>, ..., <P_N>],
    "T": [<T1>, <T2>, ..., <TN>],
    "seed": <RANDOM_SEED>,
    "strategies": [
        "completePerm",     # Null model #1, EUS
        "itemsetsSwaps",    # Null model #1, eps-AUS
        "sameSizePerm",     # Null model #2, EUS
        "sameSizeSwaps",    # Null model #2, eps-AUS
        "sameFreqSwaps",    # Null model #3, eps-AUS
        "sameSizeSeqSwaps"
    ],
    "simulated": "false",
    "datasetFilepath": <DATASET_FILEPATH>,
    "dataset": <DATASET_NAME>,  # See under SPECK/data
    "thetas": [<THETA_1>, <THETA_2>, ..., <THETA_N>],
    "outdir": "results/"
}
```

4. Run the experiment

* Sampling runtime experiment:
```sh
java -cp target/SPEck-1.0-SNAPSHOT.jar RuntimeExperiment <path to configuration file>
```

* Full SPEck run experiment:
```sh
java -cp target/SPEck-1.0-SNAPSHOT.jar SFSPExperiment <path to configuration file>
```

## To replicate our experiments, follow these steps:

1. Enter the `SPECK` directory:
```sh
cd SPECK
```

2. Run the bash script:
```sh
bash runExps.sh -e <EXP_TYPE> -d <DATA> -p <NUM_PROCS>
```

Run `bash runExps.sh -h` for a complete description of its usage.


At the end of the execution, the figures for runtime experiments will be found
in `SPECK/plots/`. The `csv` files containing the results of the full SPECK run
experiments will be found in `SPECK/results/csv` .

## License

Copyright (C) 2021-2022 Steedman Jenkins, Stefan Walzer-Goldfeld, Matteo Riondato

This work is free software: you can redistribute it and/or modify it under the
terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

The work is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the [GNU General Public License](./LICENSE) (also
available [online](https://www.gnu.org/licenses/gpl-3.0.en.html)) for more
details.
