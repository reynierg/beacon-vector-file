import logging
import os
import typing
import ujson


class OutputProcessor:
    def __init__(self, output_file_path: str):
        logging.debug(
            "%s.__init__(output_file_path=%s)",
            self.__class__.__name__,
            output_file_path,
        )
        self._output_file_path: str = output_file_path
        self._json_results_file = None
        self._results_records_count: int = 0

    def initialize(self) -> None:
        logging.debug("%s.open()", self.__class__.__name__)
        self._json_results_file = open(self._output_file_path, "w")
        self._json_results_file.write(f"[{os.linesep}")

    def close(self) -> None:
        logging.debug("%s.close()", self.__class__.__name__)
        if self._json_results_file is not None:
            self._json_results_file.write(f"{os.linesep}]{os.linesep}")
            logging.info(
                "In total, there were written '%s' records to the results document",
                self._results_records_count,
            )

            logging.info(
                "You can find a JSON file with the operation results in: '%s'",
                self._output_file_path,
            )
            self._json_results_file.close()

    def persist_record(
        self, record: typing.Dict[str, typing.Union[str, typing.List[float]]]
    ) -> None:
        """
        Appends a new record to the JSON results file.
        :param record:
            dict:
                Contains for a combination of 'BeaconId' and 'timestamp', the list of
                associated 'dbm_ant'. Example:

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

        :return:
        """

        logging.debug("%s.persist_record(record=%s)", self.__class__.__name__, record)
        if self._results_records_count > 0:
            self._json_results_file.write(f",{os.linesep}")

        ujson.dump(record, self._json_results_file, indent=4)
        self._results_records_count += 1
        logging.info(
            "Record %s was successfully append to the JSON Results file.",
            self._results_records_count,
        )
