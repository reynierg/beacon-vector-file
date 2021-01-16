"""Provides functionality for initialize the arguments parser being used, for validate
that the provided input file is accessible and for validate that the output directory
specified by the user is valid and that the user has write permission to it.

This file can be imported as a module and contains the following functions:

    * init_argparse() - initialize an ArgParser with the allowed arguments, and
    description message
    * config_logger(args_namespace) - Configures the global logger
    * validate_input_file_path(args) - verifies that the input file path is valid, and
    that the user has read permission on it
    * validate_output_directory_path(args) - verifies that the output directory is
    valid, and that the user has write permission to it
"""

import argparse
import logging
import os
import typing

from src import constants


def init_argparse() -> argparse.ArgumentParser:
    """Initialize an arguments parser, to parse the command line's arguments

    Returns
    -------
    argparse.ArgumentParser
        arguments parser to be used to process command line arguments
    """

    parser = argparse.ArgumentParser(
        description="Parse an input JSON file and create an output JSON file, with "
        "the corresponding results"
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="be verbose")

    parser.add_argument(
        "input_file_path",
        metavar="INPUT_FILE_PATH",
        type=str,
        help="the path to the input JSON file",
    )

    parser.add_argument(
        "output_directory",
        metavar="OUTPUT_DIRECTORY",
        type=str,
        help="the directory's path where the output JSON file with the results should "
        "be created",
    )

    return parser


def config_logger(args_namespace: argparse.Namespace) -> None:
    """Configures the global logger

    Parameters
    ----------
    args_namespace : argparse.Namespace
        contains the user supplied command line arguments
    """

    if args_namespace.verbose:
        logging.basicConfig(
            format=constants.DEBUG_MESSAGE_FORMAT,
            level=logging.DEBUG,
            datefmt=constants.DATE_FORMAT,
        )
    else:
        logging.basicConfig(format=constants.INFO_MESSAGE_FORMAT, level=logging.INFO)

    logging.info("The logger was successfully configured")


def validate_input_file_path(args: argparse.Namespace) -> typing.Optional[str]:
    """Verifies if the input file path is valid.

    Also will verify if user has read permission on it

    Parameters
    ----------
    args : argparse.Namespace
        Reference to an object that have the user provided command line arguments

    Returns
    -------
    str
        The absolut file path of the input file path supplied by the user
    """

    logging.debug("validate_input_file_path(args=%s)", args)
    input_file_path = args.input_file_path
    if not os.path.exists(input_file_path):
        logging.error("Specified file '%s' doesn't exist!!!", input_file_path)
        return None

    input_file_path = os.path.abspath(input_file_path)
    if not os.access(input_file_path, os.R_OK):
        logging.error(
            "Unable to access the specified input file at '%s'", input_file_path
        )
        return None

    if not os.path.isfile(input_file_path):
        logging.error("Argument '%s' must be a valid file path!!!", input_file_path)
        return None

    return input_file_path


def validate_output_directory_path(args: argparse.Namespace) -> typing.Optional[str]:
    """Verifies if the output directory path is valid.

    Also will verify if user has write permission on it

    Parameters
    ----------
    args : argparse.Namespace
        Reference to an object that have the user provided command line arguments

    Returns
    -------
    str
        The absolut directory path of the input directory path supplied by the user
    """

    logging.debug("validate_output_directory_path(args=%s)", args)
    output_directory_path = args.output_directory
    if not os.path.exists(output_directory_path):
        logging.error(
            "Specified directory '%s' doesn't exist!!!", output_directory_path
        )
        return None

    output_directory_path = os.path.abspath(output_directory_path)
    if not os.path.isdir(output_directory_path):
        logging.error(
            "Parameter '%s' must be a valid directory path!!!", output_directory_path
        )
        return None

    if not os.access(output_directory_path, os.W_OK):
        logging.error(
            "You don't have WRITE permission in the specified output directory: '%s'",
            output_directory_path,
        )
        return None

    return output_directory_path
