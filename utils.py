import argparse
import logging
import os
import typing


def init_argparse() -> argparse.ArgumentParser:
    logging.debug("init_argparse()")
    parser = argparse.ArgumentParser(
        description='Parse an input JSON file and create an output JSON file, with the corresponding results'
    )

    parser.add_argument('input_file_path',
                        metavar='input_file_path',
                        type=str,
                        help='the path to the input JSON file')

    parser.add_argument('-o',
                        '--output-directory',
                        type=str,
                        action='store',
                        default=os.path.dirname(os.path.realpath(__file__)),
                        help="the directory's path where the output JSON file with the results should be created")

    return parser


def validate_input_file_path(args: argparse.Namespace) -> typing.Optional[str]:
    logging.debug(f"validate_input_file_path(args={args})")
    input_file_path = args.input_file_path
    if not os.path.exists(input_file_path):
        logging.error(f"Specified file '{input_file_path}' doesn't exist!!!")
        return None

    input_file_path = os.path.abspath(input_file_path)
    if not os.access(input_file_path, os.R_OK):
        logging.error(f"Unable to access the specified input file at '{input_file_path}'")
        return None

    if not os.path.isfile(input_file_path):
        logging.error(f"Argument '{input_file_path}' must be a valid file path!!!")
        return None

    return input_file_path


def validate_output_directory_path(args: argparse.Namespace) -> typing.Optional[str]:
    logging.debug(f"validate_output_directory_path(args={args})")
    output_directory_path = args.output_directory
    if not os.path.exists(output_directory_path):
        logging.error(f"Specified directory '{output_directory_path}' doesn't exist!!!")
        return None

    output_directory_path = os.path.abspath(output_directory_path)
    if not os.path.isdir(output_directory_path):
        logging.error(f"Parameter '{output_directory_path}' must be a valid directory path!!!")
        return None

    if not os.access(output_directory_path, os.W_OK):
        logging.error(f"You don't have WRITE permission in the specified output directory: '{output_directory_path}'")
        return None

    return output_directory_path
