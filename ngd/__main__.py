import __init__
import argparse
import os
import sys

from datalist import data_list
from download_files import download


def main():
    """
    Main function.
    """

    os.system("cls" if os.name == "nt" else "clear")

    # Setting PROGRAM_NAME
    parser = argparse.ArgumentParser(
        prog=__init__.__program_name__,
        prefix_chars="**-",
        epilog=__init__.__program_name__ + "-v1.0",
        usage=__init__.__program_name__
        + "\nExemple for usage:\n$ python %(prog)s -start 2010-01-01 -end 2010-01-02 -interval 30min\n"
        "\nor type '%(prog)s -h' for more information.\n ",
        description="Global parameters",
    )

    # Add arguments
    subparsers = parser.add_subparsers()

    # Data List
    parser_track = subparsers.add_parser("data_list", help="List of all data list")
    parser_track.set_defaults(func=data_list)

    # Download
    parser_track = subparsers.add_parser("download", help="Download Mode")
    parser_track.set_defaults(func=download)

    # Add parameters
    # Start date
    parser.add_argument(
        "-start",
        type=str,
        metavar='',
        help="Start date in format YYYY-MM-DD hh:mm",
        required=False,
    )
    # End date
    parser.add_argument(
        "-end",
        type=str,
        metavar='',
        help="End date in format YYYY-MM-DD hh:mm",
        required=False,
    )

    # Interval
    parser.add_argument(
        "-interval",
        type=str,
        metavar='',
        help="Interval in format. Ex: 30min, 1h, 1d, 1w, 1m, 1y",
        default="30min",
        required=False,
    )

    # Ignore times between midnight and 6am
    parser.add_argument(
        "-ignore_times",
        type=str,
        metavar='',
        help="Ignore times between: Ex: Ignore between 8pm and 6am -> 20:00,06:00",
        default="None",
        required=False,
    )

    # Satellite
    parser.add_argument(
        "-provider",
        type=str,
        metavar='',
        help="NOAA or DSA(INPE)",
        required=False,
    )

    # Satellite
    parser.add_argument(
        "-sat",
        type=str,
        metavar='',
        help="Satellite name",
        required=False,
    )

    # Sensor
    parser.add_argument(
        "-sensor",
        type=str,
        metavar='',
        help="Sensor name or channel",
        required=False,
    )

    # Boundbox
    parser.add_argument(
        "-bbox",
        type=str,
        metavar='',
        help="Bounding box in format min_lon,min_lat,max_lon,max_lat",
        required=False,
    )

    # Proj4
    parser.add_argument(
        "-proj4",
        type=str,
        metavar='',
        help="Cartographic projection. Default is EPSG:4326 (+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs)",
        default="+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
        required=False,
    )

    # Output path
    parser.add_argument(
        "-output",
        type=str,
        metavar='',
        help="Output Directory",
        default=os.getcwd()+"/output",
        required=False,
    )

    # Add all parsers to option
    options = parser.parse_args()

    # For option in options:
    for k, v in options.__dict__.items():
        # Add to envrionment
        os.environ[k] = str(v)

    # If not argument, print help
    if len(sys.argv) <= 1:
        print(parser.format_help())
        return 0

    # Check if function is defined
    if hasattr(options, "func"):
        # Run function
        options.func()
    else:
        print("Invalid function\n\n")
        print(parser.format_help())
        return 0


if __name__ == '__main__':
    main()
