Beacon-vector-file generator

- [DESCRIPTION](#description)
- [SOLUTION](#solution)
- [REFERENCES](#references)
- [DEPENDENCIES](#dependencies)  
- [INSTALLATION](#installation)
- [RUN](#run)
- [OPTIONS](#options)
- [EXAMPLES](#examples)

# DESCRIPTION

The goal of this exercise is to generate a list that contains all beacons at certain timestamps with their corresponding support vector.

- As input, we have a database which we collected during a beacon tracking test.
- The table is made from logs of signal levels (in dbm) from different Wi-Fi antennas.
- At every timestamp we did a recording of each of these antennas in a way that yields table entries.

Data sample:

```json
[
  {
    "BeaconId": 101,
    "ant_id": 103,
    "dbm_ant": -68.74636830519334,
    "timestamp": "1999-06-17 00:11:00"
  },
  {
    "BeaconId": 101,
    "ant_id": 101,
    "dbm_ant": -77.80792406374334,
    "timestamp": "1999-06-17 00:11:00"
  },
  {
    "BeaconId": 101,
    "ant_id": 106,
    "dbm_ant": -53.139973206690684,
    "timestamp": "1999-06-17 00:11:00"
  },
  {
    "BeaconId": 303,
    "ant_id": 102,
    "dbm_ant": -84.76679099514944,
    "timestamp": "1999-06-17 00:12:00"
  },
  {
    "BeaconId": 101,
    "ant_id": 105,
    "dbm_ant": -19.698948991976884,
    "timestamp": "1999-06-17 00:11:00"
  },
  {
    "BeaconId": 303,
    "ant_id": 101,
    "dbm_ant": -46.17761120301083,
    "timestamp": "1999-06-17 00:12:00"
  },
  {
    "BeaconId": 303,
    "ant_id": 104,
    "dbm_ant": -68.58154321681343,
    "timestamp": "1999-06-17 00:12:00"
  }
]
```

In order to do further calculations we need a file which maps the beacon id + timestamp to a given array of antennas (see ``[201,202,203,204,205,206]``, please keep the order in the array) with all the recorded dbm values (if a dbm value is missing a default value of ``-135`` should be assigned).

- Please write a script in `main.py` which outputs the vectors for all beacons.
- Do not assume that the beacons, nor the timestamps will be sorted.
- The input file can be found in ``input.json``.
- The output can be logged to the console or be written to a file.


Sample output based on the data sample above and with antenna IDs [101,102,103,104,105,106]:

```js
[
  {
    "beacon": "101, 1999-06-17T00:11:00.000Z",
    "vector": [
      -77.80792406374334,
      -135,
      -68.74636830519334,
      -135,
      -19.698948991976884,
      -53.139973206690684
    ]
  },
  {
    "beacon": "303, 1999-06-17T00:12:00.000Z",
    "vector": [
      -46.17761120301083,
      -84.76679099514944,
      -135,
      -68.58154321681343,
      -135,
      -135
    ]
  }
]
```

**BONUS:** The table we use in production has more than 3 Million entries. Write the code, so it can handle a large 
amount of IO data.

# SOLUTION

Because according to the problem's specifications, no assumption should be made related to that the beacons, nor the 
timestamps will be sorted in the input file, and the input JSON file could be huge; it's very important to use some kind 
of intermediate storage, where could be stored temporarily, for every beacon the vector of dbm_ant for the antennas, 
before persist it to the final JSON output file.

Based on that, it was decided to use the Hierarchical Data Format version 5 (HDF5) which is an open source binary file 
format that supports large, complex, heterogeneous data.

# REFERENCES:

[Hierarchical Data Format](https://en.wikipedia.org/wiki/Hierarchical_Data_Format)

[HDF5 for Python](https://www.h5py.org/)

# DEPENDENCIES

- numpy
- h5py
- pydantic
- ujson

# INSTALLATION 

To install the required dependencies:

Clone the project executing the following command in a terminal:\
`git clone https://github.com/reynierg/beacon-vector-file.git`

Move to the project's directory "beacon-vector-file":\
`cd beacon-vector-file`

Create a virtual environment with Python >= 3.6:\
`python3 -m venv venv`
  
Activate the created virtual environment:\
`. venv/bin/activate`  

Install the project's dependencies:  
`pip install -r requirements.txt`

# RUN
In the same terminal where were executed the previous commands, execute the following:\
`python bin/extract_beacons_vectors.py [OPTIONS] <INPUT_FILE_PATH> <OUTPUT_DIRECTORY>`

# OPTIONS
    -h, --help                  show this help text and exit

# EXAMPLES
Process the `input.json` file that is in the current directory, and write the output to 
a new file named `results.json` in the same directory. Notice the last point indicating that the OUTPUT_DIRECTORY should be the current directory:\
`python bin/extract_beacons_vectors.py input.json .`

Process the `input.json` file that is in the current directory, and write the output to 
a new file named `results.json` in the directory `/home/userX/`:\
`python bin/extract_beacons_vectors.py input.json /home/userX/`
