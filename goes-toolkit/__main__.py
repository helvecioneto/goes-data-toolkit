import __init__
import argparse
import os
import sys

from datalist import data_list
from process import process_files


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
        + "\npython goes-toolkit -s '2020-06-06 08:00' -e '2020-06-06 22:00' -bt '09:00,22:00' -i 10min -sat goes16 -c 02 -p AWS download\n"
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
    parser_track.set_defaults(func=process_files)

    # Add parameters
    # Start date
    parser.add_argument(
        "-s",
        type=str,
        metavar='',
        help="Start date in format YYYY-MM-DD hh:mm",
        required=False,
    )
    # End date
    parser.add_argument(
        "-e",
        type=str,
        metavar='',
        help="End date in format YYYY-MM-DD hh:mm",
        required=False,
    )

    # Interval
    parser.add_argument(
        "-i",
        type=str,
        metavar='',
        help="Interval in format. Ex: 30min, 1h, 1d, 1w, 1m, 1y",
        default="10min",
        required=False,
    )

    # Ignore times between midnight and 6am
    parser.add_argument(
        "-bt",
        type=str,
        metavar='',
        help="Between times Ex:> 20:00,06:00",
        default="None",
        required=False,
    )

    # Satellite
    parser.add_argument(
        "-p",
        type=str,
        metavar='',
        help="NOAA, Amazon (AWS) or DSA(INPE)",
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
        "-c",
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
        default="-74.000000,-35.000000,-30.000000,8.000000", # ATLAS NOVO
        # default="-81.400000,-35.000000,-30.000000,12.600000", # ATLAS OLD
        # default="-40.48,-9.48,-34.6,-5.52",  # Paraiba
        # default="-50.899658,-2.040279,-43.549805,2.284551",  # Belém
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
        "-o",
        type=str,
        metavar='',
        help="Output Directory",
        default=os.getcwd()+"/output",
        required=False,
    )

    # Add product if data is AWS
    parser.add_argument(
        "-prod",
        type=str,
        metavar='',
        help="Output Directory",
        default='ABI-L2-CMIPF',
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
