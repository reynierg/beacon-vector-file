import logging
import os
import sys

import constants
from hdf5_storage import HDF5Storage
from output_processor import OutputProcessor
import utils


def main() -> None:
    logging.debug("main()")
    parser = utils.init_argparse()
    args = parser.parse_args()
    input_file_path = utils.validate_input_file_path(args)
    if not input_file_path:
        sys.exit()

    output_directory_path = utils.validate_output_directory_path(args)
    if not output_directory_path:
        sys.exit()

    output_file_path = os.path.join(output_directory_path, constants.RESULTS_FILE_NAME)
    logging.info(f"The output JSON file will be saved in the following path: '{output_file_path}'")

    output_processor = OutputProcessor(output_file_path)
    with HDF5Storage(input_file_path,
                     output_processor,
                     constants.DEFAULT_DBM_ANT_VALUE,
                     constants.ANTENNA_IDS) as main_object:
        main_object.parse_json_documents_from_file()
        main_object.persist_beacons_vectors_to_results_file()


if __name__ == "__main__":
    if constants.LOG_LEVEL == logging.DEBUG:
        logging.basicConfig(format=constants.DEBUG_MESSAGE_FORMAT,
                            level=constants.LOG_LEVEL,
                            datefmt=constants.DATE_FORMAT)
    else:
        logging.basicConfig(format=constants.INFO_MESSAGE_FORMAT,
                            level=constants.LOG_LEVEL)

    main()
