"""Contains logic to store in a HDF5 file format every beacon

Every beacon is stored with its associated vector of dbm_ant values

This file can be imported as a module and contains the following classes:
    * HDF5Storage - provides storage and retrieval of the beacons data in a HDF5 file
"""

import json
import logging
import os
import tempfile
import types
import typing

import h5py
import pydantic

from src.models import JSONDocumentModel
from src.output_processor import OutputProcessor


class HDF5Storage:
    """A class used for the storage and retrieval of the beacons data in a HDF5 file

    Attributes
    ----------
    ANTENNA_ID_DBM_MAP_GROUP_NAME : str
        a string to be used as a HDF5's group name, for a Group where will be stored
        for every beacon's antenna, its id and associated dbm_ant value.

    SAMPLED_ANTENNAS_COUNT_ATTR_NAME: str
        a string to be used as a name, for the attribute of an existent
        hdf5_antenna_id_dbm_group, for keep track of how many antennas has been stored
        for a beacon.
    """

    ANTENNA_ID_DBM_MAP_GROUP_NAME = "antenna_id_dbm_map"
    SAMPLED_ANTENNAS_COUNT_ATTR_NAME = "sampled_antennas_count"

    def __enter__(self) -> "HDF5Storage":
        """Context Manager to ensure the closure of open files

        Returns
        -------
        HDF5Storage:
            the instance of the HDF5Storage being used as a context manager
        """

        logging.debug("%s.__enter__()", self.__class__.__name__)
        try:
            # Use a temporal file, so that when the application finish, and the
            # temporal file is closed, the temporal file will be automatically
            # deleted. Also if for some reason we can't delete it, the O.S will take
            # care of delete it for us:
            self._tmp_file = tempfile.TemporaryFile()
            self._hdf5_root_file: h5py.File = h5py.File(self._tmp_file, "w")
            logging.debug("self._hdf5_root_file.name: %s", self._hdf5_root_file.name)

            # In the "beacons" Group, will be created a sub-group for every
            # unique combination of BeaconId and timestamp:
            self._hdf5_beacons_group = self._hdf5_root_file.create_group("beacons")
            logging.debug(
                "self._hdf5_beacons_group.name: %s", self._hdf5_beacons_group.name
            )

            # We will store the results with the required JSON
            # structure in a file in constants.RESULT_FILE_PATH:
            self._output_processor.initialize()
            return self
        except Exception:
            logging.exception("Error:")

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_val: typing.Optional[BaseException],
        exc_tb: typing.Optional[types.TracebackType],
    ) -> typing.Optional[bool]:
        """Closes the used files

        Closes the HDF5 file backed by the the temp file, as well as the results JSON
        file.

        Parameters
        ----------
        exc_type : typing.Optional[typing.Type[BaseException]]
            Type of the exception that caused the context to be exited
        exc_val : typing.Optional[BaseException]
            The exception that caused the context to be exited
        exc_tb : typing.Optional[types.TracebackType]
            Traceback related to the call stack associated to the exception

        Returns
        -------
        typing.Optional[bool]:
            If an exception is supplied, and the method wishes to suppress the
            exception (i.e., prevent it from being propagated), it should return a true
            value. Otherwise, the exception will be processed normally upon exit from
            this method.
        """

        logging.debug(
            "%s.__exit__(exc_type=%s, exc_val=%s, exc_tb=%s)",
            self.__class__.__name__,
            exc_type,
            exc_val,
            exc_tb,
        )
        try:
            self._hdf5_root_file.close()
            self._tmp_file.close()
        except Exception:
            logging.exception("Error:")

        try:
            self._output_processor.close()
        except Exception:
            logging.exception("Error:")

        return False

    def __init__(
        self,
        input_json_file_path: str,
        out_processor: OutputProcessor,
        default_dbm_ant_value: int,
        expected_antenna_ids: typing.List[int],
    ):
        """
        Parameters
        ----------
        input_json_file_path : str
            The full file path of the input JSON file to process
        out_processor : OutputProcessor
            Handles the storage of a record, that contains a beacon's associated
            antennas dbm_values
        default_dbm_ant_value : int
            Default value to be used as dbm_ant reading associated to an antenna id,
            when in the input file, was not found the corresponding dbm_ant for an
            antenna of a beacon.
        expected_antenna_ids : typing.List[int]
            Contains the list of antennas ids, for which should be found in the input
            file, the corresponding readings for its dbm_ant value.
        """

        logging.debug(
            "%s.__init__(input_json_file_path=%s, default_dbm_ant_value=%s, "
            "expected_antenna_ids=%s)",
            self.__class__.__name__,
            input_json_file_path,
            default_dbm_ant_value,
            expected_antenna_ids,
        )

        self._input_json_file_path: str = input_json_file_path
        self._output_processor: OutputProcessor = out_processor
        self._default_dbm_ant_value: float = default_dbm_ant_value
        self._expected_antenna_ids: typing.List[int] = expected_antenna_ids

        # Used for HDF5: Storage of huge datasets. Here will be used for storage of
        # the records of the input JSON file:
        self._tmp_file: typing.Optional[tempfile.TemporaryFile] = None
        self._hdf5_root_file: typing.Optional[h5py.File] = None
        self._hdf5_beacons_group: typing.Optional[h5py.Group] = None

        self._records_parsed_count: int = 0
        self._results_records_count: int = 0

    def _build_results_record(
        self,
        hdf5_antenna_id_dbm_group: h5py.Group,
        beacon_key: str,
        json_record: typing.Optional[JSONDocumentModel] = None,
    ) -> typing.Dict[str, typing.Union[str, typing.List[float]]]:
        """Builds a result record to be saved in the output JSON file.

        It extracts from the HDF5 file, all the ant_id, dbm_ant pairs associated to the
        sub-group referenced by `hdf5_antenna_id_dbm_group`, where the ant_id match a
        value in the constants.ANTENNA_IDS list. The dbm_ant associated is used to
        construct the beacon's dbm_ant vector.

        Parameters
        ----------
        hdf5_antenna_id_dbm_group: h5py.Group
            Reference to a Group that contains for a beacon, the antennas ids with its
            associated dbm_ant readings.
        beacon_key : str
            Contains a value that identifies a beacon. It's associated with the "beacon"
            key in a generated beacon record.
        json_record : typing.Optional[JSONDocumentModel]
            When not None, will reference a model object with a record's data, parsed
            from the input file.

        Returns
        -------
        typing.Dict[str, typing.Union[str, typing.List[float]]]
            a dict wit a beacon_key and its associated vector of dbm_and reading.
            Example:
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
            }
        """

        logging.debug("%s._build_results_record(...)", self.__class__.__name__)
        # Sort dbm_ant vector in accordance to constants.ANTENNA_IDS sorting, and fill
        # with a default value, for the absent antenna_id:
        dbm_ant_vector = []
        for expected_ant_id in self._expected_antenna_ids:
            # Try to get from the group referred by `hdf5_antenna_id_dbm_group` in the
            # HD5F file, a dataset related to `expected_ant_id`
            dbm_ant = hdf5_antenna_id_dbm_group.get(str(expected_ant_id))
            # logging.debug("dbm_ant=%s", dbm_ant)
            if dbm_ant is None:
                # A `dbm_ant` was not found in the HDF5 file for the `expected_ant_id`.
                # If `json_record` is not None, check if its `json_record.ant_id`
                # match `expected_ant_id`, then if either `json_record` is None or
                # `json_record.ant_id` doesn't match, use the default value for
                # dbm_ant:
                dbm_ant = (
                    json_record.dbm_ant
                    if json_record is not None and expected_ant_id == json_record.ant_id
                    else self._default_dbm_ant_value
                )
            else:
                # A `dbm_ant` that match `expected_ant_id` was found in the HDF5 file.
                # Extract the float scalar from the HDF5 corresponding dataset:
                dbm_ant = dbm_ant[()]

            dbm_ant_vector.append(dbm_ant)

        logging.debug("dbm_ant_vector=%s", dbm_ant_vector)
        result_record = {"beacon": beacon_key, "vector": dbm_ant_vector}
        logging.debug("record=%s", result_record)
        return result_record

    def _persist_antenna_id_and_dbm_value_in_hdf5(
        self,
        hdf5_antenna_id_dbm_group: h5py.Group,
        json_record: JSONDocumentModel,
        already_sampled_antennas_count: int = 0,
    ) -> None:
        """Persist the ant_id and dbm_ant in the HDF5 file.

        Extracts from the JSON document, the ant_id and dbm_ant, and persist them as a
        key-value pair in the sub-group referenced by `hdf5_antenna_id_dbm_group`. It
        will also update/create an attribute associated to the
        `hdf5_antenna_id_dbm_group`, being used for track the count of antennas that
        have been already processed:

        Parameters
        ----------
        hdf5_antenna_id_dbm_group : h5py.Group
            Reference to a Group that contains for a beacon, the antennas ids with its
            associated dbm_ant readings.
        json_record : JSONDocumentModel
            Reference to a model object with a record's data, parsed from the input
            file.
        already_sampled_antennas_count : int
            Contains the number of antennas for which its associated dbm value, has
            been already stored in the corresponding hdf5_antenna_id_dbm_group.
        """

        logging.debug(
            "%s._persist_antenna_id_and_dbm_value_in_hdf5(...)", self.__class__.__name__
        )
        ant_id = str(json_record.ant_id).encode("utf-8")
        logging.debug(
            "The key-value pair (%s, %s) will be saved in the '%s' sub-group",
            ant_id,
            json_record.dbm_ant,
            self.ANTENNA_ID_DBM_MAP_GROUP_NAME,
        )
        hdf5_antenna_id_dbm_group[ant_id] = json_record.dbm_ant
        # Update/Create the attribute to track the count of antennas being already
        # tracked:
        logging.debug(
            "Adding 1 to hdf5_antenna_id_dbm_group.attrs[%s]",
            self.SAMPLED_ANTENNAS_COUNT_ATTR_NAME,
        )
        hdf5_antenna_id_dbm_group.attrs[self.SAMPLED_ANTENNAS_COUNT_ATTR_NAME] = (
            already_sampled_antennas_count + 1
        )
        logging.debug(
            "After add 1: hdf5_antenna_id_dbm_group"
            ".attrs[self.SAMPLED_ANTENNAS_COUNT_ATTR_NAME] is %s",
            hdf5_antenna_id_dbm_group.attrs[self.SAMPLED_ANTENNAS_COUNT_ATTR_NAME],
        )

    def _process_antennas_samples_statistics(
        self,
        hdf5_antenna_id_dbm_group: h5py.Group,
        beacon_key: str,
        json_record: JSONDocumentModel,
    ) -> None:
        """It process a json_record associated to a beacon.

        Depending on if it is or not, for the beacon the reading of dbm_ant for the
        last of its antennas that was pending; either an output record will be created
        and hand over to the output processor, or the reading will be stored in the
        corresponding hdf5_antenna_id_dbm_group associated to the beacon_key in the
        file in HDF5 format

        Parameters
        ----------
        hdf5_antenna_id_dbm_group : h5py.Group
            Reference to a Group that contains for a beacon, the antennas ids with its
            associated dbm_ant readings.
        beacon_key : str
            Contains a value that identifies a beacon. It's associated with the "beacon"
            key in a generated beacon record.
        json_record : JSONDocumentModel
            Reference to a model object with a record's data, parsed from the input
            file.
        """

        logging.debug(
            "%s._process_antennas_samples_statistics(...)", self.__class__.__name__
        )
        already_sampled_antennas_count = (
            hdf5_antenna_id_dbm_group.attrs.get(self.SAMPLED_ANTENNAS_COUNT_ATTR_NAME)
            or 0
        )
        logging.debug(
            "already_sampled_antennas_count: %s", already_sampled_antennas_count
        )
        if already_sampled_antennas_count + 1 == len(self._expected_antenna_ids):
            # This is the last expected sampled antenna for this beacon in the
            # corresponding timestamp, it's time to dump the corresponding record to
            # the JSON results file, we don't want to keep it stored in the HDF5 file
            # unnecessarily until the whole huge JSON file gets complete parsed:
            logging.debug(
                "already_sampled_antennas_count + 1 == "
                "len(self._expected_antenna_ids):"
            )
            result_record = self._build_results_record(
                hdf5_antenna_id_dbm_group, beacon_key, json_record
            )

            # Append the record to the JSON results file:
            self._output_processor.persist_record(result_record)
            # Remove the beacon's associated Group, and all its sub-groups from the
            # storage, they're not necessary any more:
            del self._hdf5_beacons_group[beacon_key]
        else:
            # Store the key/value pair with the new ant_id and dbm_ant:
            logging.debug(
                "already_sampled_antennas_count + 1 != "
                "len(self._expected_antenna_ids):"
            )
            self._persist_antenna_id_and_dbm_value_in_hdf5(
                hdf5_antenna_id_dbm_group, json_record, already_sampled_antennas_count
            )

    def _process_json_record(self, json_record: JSONDocumentModel) -> None:
        """Process a JSON document record

        Process a JSON document parsed from the input file, storing the corresponding
        information in a HDF5 file. HDF5 is being used because it's a smart data
        container, well suited for storing large arrays of numbers, typically
        scientific datasets. Here will be used for store data related to the huge JSON
        file being processed.

        If required, it will create a new beacon Group and an associated antenna_id_dbm
        sub-group.

        Parameters
        ----------
        json_record : JSONDocumentModel
            Reference to a model object with a record's data, parsed from the input
            file.
        """

        logging.debug("%s._process_json_record(...)", self.__class__.__name__)
        # HDF5 beacons sub-groups will have a name with the pattern: "Beaconid,
        # timestamp":
        beacon_key = f"{json_record.beacon_id}, {json_record.timestamp}"
        logging.debug("beacon_key=%s", beacon_key)
        # Try first to get an existing HDF5 sub-group with the name hold by
        # `beacon_key`, from the group referred by `self._hdf5_beacons_group`:
        hdf5_beacon_group = self._hdf5_beacons_group.get(beacon_key)
        if hdf5_beacon_group is not None:
            # A Sub-Group already exist with this BeaconId and timestamp:
            logging.debug(
                "A Beacon group with beacon_key '%s' already exist", beacon_key
            )
            # An existing beacon sub-group always have an associated antenna_id_dbm
            # sub-group with key, value pairs; for its associated ant_id and dbm_ant:
            hdf5_antenna_id_dbm_group = hdf5_beacon_group.get(
                self.ANTENNA_ID_DBM_MAP_GROUP_NAME
            )
            if hdf5_antenna_id_dbm_group is not None:
                logging.debug(
                    "hdf5_antenna_id_dbm_group with id '%s' already exist",
                    self.ANTENNA_ID_DBM_MAP_GROUP_NAME,
                )
                # A sub-group already exist for the `hdf5_beacon_group`'s tracked
                # antennas dbm values:
                # We will add to it, the ant_id/dbm_ant pair received in the
                # `json_record`, also we will explore the sub-group to determine if
                # the record is complete, and ready to be append to the results JSON
                # file. Complete means, that with the received ant_id/dbm_ant pair,
                # it now already has all the data related to the expected
                # `constants.ANTENNA_IDS`. If the record is complete, it will be saved
                # in the JSON output file:
                self._process_antennas_samples_statistics(
                    hdf5_antenna_id_dbm_group, beacon_key, json_record
                )
            else:
                logging.warning(
                    "The Group named '%s' doesn't have a sub-group " "named '%s'",
                    beacon_key,
                    self.ANTENNA_ID_DBM_MAP_GROUP_NAME,
                )
        else:
            # A sub-group doesn't already exist with this BeaconId and timestamp:
            # Create a new sub-group for the new beacon amd timestamp:
            logging.debug(
                "A Beacon sub-group with beacon_key '%s' doesn't exist. It "
                "will be created",
                beacon_key,
            )
            hdf5_beacon_group = self._hdf5_beacons_group.create_group(beacon_key)
            logging.debug(
                "A Beacon sub-group with beacon_key '%s' was created", beacon_key
            )
            # Create a Sub-Group for the hdf5_beacon_group's tracked antennas dbm
            # values:
            logging.debug(
                "Trying to create the associated sub-group for '%s'",
                self.ANTENNA_ID_DBM_MAP_GROUP_NAME,
            )
            hdf5_antenna_id_dbm_group = hdf5_beacon_group.create_group(
                self.ANTENNA_ID_DBM_MAP_GROUP_NAME
            )
            logging.debug(
                "A sub-group of '%s', with name '%s' was successfully " "created",
                beacon_key,
                self.ANTENNA_ID_DBM_MAP_GROUP_NAME,
            )

            # Persist the ant_id, dbm_ant key/value pair:
            self._persist_antenna_id_and_dbm_value_in_hdf5(
                hdf5_antenna_id_dbm_group, json_record
            )

    def persist_beacons_vectors_to_results_file(self) -> None:
        """Extract beacons vectors from the HDF5 file.

        The extracted beacons vectors will be persisted in the JSON results file
        """

        logging.debug(
            "%s.persist_beacons_vectors_to_results_file()", self.__class__.__name__
        )
        # Iterate over all the HDF5 beacons groups from the HDF5 file:
        for beacon_group_name in self._hdf5_beacons_group:
            logging.debug("beacon_group_name: %s", beacon_group_name)
            # Try to get a reference to a beacon HDF5 group with name
            # `beacon_group_name`:
            hdf5_beacon_group = self._hdf5_beacons_group.get(beacon_group_name)
            if hdf5_beacon_group is None:
                logging.warning(
                    "A HDF5 group with name '%s' was not found", beacon_group_name
                )
                continue

            logging.debug("hdf5_beacon_group's type: %s", type(hdf5_beacon_group))
            # logging.debug("hdf5_beacon_group's value: %s", hdf5_beacon_group)
            # Try to get a reference to the beacon's sub-group "antenna_id_dbm_map":
            hdf5_antenna_id_dbm_map_group = hdf5_beacon_group.get(
                self.ANTENNA_ID_DBM_MAP_GROUP_NAME
            )
            if hdf5_antenna_id_dbm_map_group is None:
                logging.warning(
                    "A HDF5 group with name '%s' was not found",
                    self.ANTENNA_ID_DBM_MAP_GROUP_NAME,
                )
                continue

            logging.debug(
                "hdf5_antenna_id_dbm_map_group's type: %s",
                type(hdf5_antenna_id_dbm_map_group),
            )
            # Extract the ant_id, dbm_ant key/value pairs from the
            # `hdf5_antenna_id_dbm_map_group` and build a JSON document with them and
            # the beacon id and timestamp:
            result_record = self._build_results_record(
                hdf5_antenna_id_dbm_map_group, beacon_group_name
            )

            # Append the record to the JSON results file:
            self._output_processor.persist_record(result_record)
            # Remove the beacon's associated Group, and all his sub-groups from the
            # HDF5 file storage, they're not necessary any more:
            del hdf5_beacon_group

    def parse_json_documents_from_file(self) -> None:
        """Reads one line at a time from the input JSON file, and process it."""

        logging.debug("%s.parse_json_documents_from_file()", self.__class__.__name__)
        # The JSON file could be huge, so it must be parsed one line at a time, for
        # not get OutOfMemory exception:
        with open(self._input_json_file_path, "r") as input_json_file:
            line_index = 1
            processed_json_documents_count = 0
            for line in input_json_file:
                line = line.strip()
                logging.debug("Line '%s''s content after strip spaces:", line_index)
                logging.debug(line)

                if not line or line == os.linesep:
                    logging.warning(
                        "Line '%s' is empty, it will be ignored", line_index
                    )
                    line_index += 1
                    continue

                if line in ("]", f"]{os.linesep}"):
                    logging.debug(
                        "The end of the file was found at line #'%s'", line_index
                    )
                    logging.info(
                        "In total, there were processed %s JSON documents",
                        processed_json_documents_count,
                    )
                    break

                if line in ("[", f"[{os.linesep}"):
                    logging.debug(
                        "The first line of the file was found at line #'%s'", line_index
                    )
                    line_index += 1
                    continue

                try:
                    # Looking for the index of the character that close a JSON
                    # document definition:
                    open_brace_idx = line.index("{")
                    close_brace_idx = line.rindex("}")
                    if open_brace_idx != 0:
                        logging.warning(
                            "Line #'%s' is malformed. The open curly brace character "
                            "'{' must be the first in the every line",
                            line_index,
                        )
                        line_index += 1
                        continue
                except ValueError:
                    # Line is malformed:
                    logging.exception(
                        "Line #'%s' is malformed. Every JSON document should be in "
                        "its own line:",
                        line_index,
                    )
                    line_index += 1
                    continue

                # Ignore any character(including the ',' character) that appears after
                # the '}' character, at the end
                # of the line:
                line = line[: close_brace_idx + 1]
                json_document: typing.Optional[
                    JSONDocumentModel
                ] = self.parse_text_line(line, line_index)
                line_index += 1
                if json_document is None:
                    # Line doesn't contains a valid JSON document:
                    continue

                # Process the Python dict with the JSON document data:
                self._process_json_record(json_document)
                processed_json_documents_count += 1

    def parse_text_line(
        self, line: str, line_index: int
    ) -> typing.Optional[JSONDocumentModel]:
        """Tries to parse from a string a beacon input record.

        It will first try to load a JSON document from the string, and then initialize
        a JSONDocumentModel with it.

        Parameters
        ----------
        line : str
            A string line that should contains a JSON document with a beacon data
        line_index : int
            Represents the base 0 index, of the string line in the input file

        Returns
        -------
        typing.Optional[JSONDocumentModel]
            If from the line could be loaded a JSON document and contains the expected
            fields, will be returned a JSONDocumentModel instance with the data.
            If not, None will be returned.
        """

        logging.debug("%s.parse_text_line(...)", self.__class__.__name__)
        try:
            # Deserialize a text line containing a JSON document, to a Python dict:
            data = json.loads(line)
            # Validate the JSON document structure, and use the model to access its
            # content:
            return JSONDocumentModel(**data)
        except json.JSONDecodeError:
            logging.warning(
                "JSON document in line #'%s' is malformed. It will be ignored",
                line_index,
            )
            return None
        except pydantic.ValidationError:
            logging.warning(
                "JSON document in line '%s' is invalid or malformed. It must contain "
                "all/just the expected fields. It will be ignored",
                line_index,
            )
            return None
