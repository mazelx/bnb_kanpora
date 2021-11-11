#!/usr/bin/python3
# ============================================================================
# Airbnb web site scraper, for analysis of Airbnb listings
# Tom Slee, 2013--2015.
#
# function naming conventions:
#   ws_get = get from web site
#   db_get = get from database
#   db_add = add to the database
#
# function name conventions:
#   add = add to database
#   display = open a browser and show
#   list = get from database and print
#   print = get from web site and print
# ============================================================================
import logging
import argparse
import sys

from airbnb_collector.config import ABConfig
from airbnb_collector.controllers import ABSurvey, ABListing, ABDatabaseController, ABSurveyController, ABSearchAreaController

# ============================================================================
# CONSTANTS
# ============================================================================

# Script version

# 4.0 Nov 2021: WIP

# 3.6 May 2019: Fixed problem where pagination was wrong because of a change in 
# the Airbnb web site.
# 3.5 July 2018: Added column to room table for rounded-off latitude and
# longitude, and additional location table for Google reverse geocode addresses
# 3.4 June 2018: Minor tweaks, but now know that Airbnb searches do not return
#                listings for which there are no available dates.
# 3.3 April 2018: Changed to use /api/ for -sb if key provided in config file
# 3.2 April 2018: fix for modified Airbnb site. Avoided loops over room types
#                 in -sb
# 3.1 provides more efficient "-sb" searches, avoiding loops over guests and
# prices. See example.config for details, and set a large max_zoom (eg 12).
# 3.0 modified -sb searches to reflect new Airbnb web site design (Jan 2018)
# 2.9 adds resume for bounding box searches. Requires new schema
# 2.8 makes different searches subclasses of ABSurvey
# 2.7 factors the Survey and Listing objects into their own modules
# 2.6 adds a bounding box search
# 2.5 is a bit of a rewrite: classes for ABListing and ABSurvey, and requests lib
# 2.3 released Jan 12, 2015, to handle a web site update
SCRIPT_VERSION_NUMBER = "3.7.0"
# logging = logging.getLogger()

class ABCollectorApp:

    def __init__(self):
        """
        Read and parse command-line arguments]
        """
        parser = argparse.ArgumentParser(   
            description='Manage a database of Airbnb listings.',
            usage='''%(prog)s <command> [<args>]
        
            Available commands:

                survey [add|delete|list|run|run_extra]
                search_area [add|delete|list]
                db [--check]

            Optional args:
                -v | --verbose
                -c | --config file <config_file>
                -V | --version
                -? | --help
    ''')

        parser.add_argument('command', help='Command to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    def parse_subcommand_args(self, parser):
        parser.add_argument("-v", "--verbose",
                            action="store_true", default=False,
                            help="""write verbose (debug) output to the log file""")
        parser.add_argument("-c", "--config_file",  
                            metavar="config_file", action="store", default=None,
                            help="""explicitly set configuration file, instead of
                            using the default <username>.config""")
        parser.add_argument('-V', '--version',
                            action='version',
                            version='%(prog)s, version ' +
                            str(SCRIPT_VERSION_NUMBER))
        parser.add_argument('-?', action='help')
        parser.add_argument('subcommand')

        # TWO argvs, ie the command and the subcommand (commit)
        args = parser.parse_args(sys.argv[2:]) 

        if(args.verbose):
            print("with verbose")
        if(args.config_file):
            print(f"with config {args.config_file}")
        
        return args

    def db(self):
        parser = argparse.ArgumentParser(
            description='Manage an airbnb survey')
        args = self.parse_subcommand_args(parser)

        db = ABDatabaseController(args.config)

        if(args.subcommand == "check"):            
            db.db_check_connection()
        else:
            print("Unrecognized subcommand")
            parser.print_help()
            exit(1)

    def survey(self):
        parser = argparse.ArgumentParser(description='Manage an airbnb survey')
        args = self.parse_subcommand_args(parser)

        if(args.subcommand == "add"):
            search_area_id = input("search_area_id : ")
            ABSurveyController(self.config).add_survey(search_area_id)

        elif(args.subcommand == "delete"):
            survey_id = input("survey_id : ")
            question = "Are you sure you want to delete listings for survey {}? [y/N] ".format(survey_id)
            sys.stdout.write(question)
            choice = input().lower()
            if choice != "y":
                print("Cancelling the request.")
                return
            ABSurveyController.delete_survey(survey_id)

        elif(args.subcommand == "list"):
            print("list survey")
        elif(args.subcommand == "run"):
            survey_id = input("survey_id : ")
            ABSurveyController(args.config).run(survey_id)
        elif(args.subcommand == "run_extra"):
            print("run extra information search for survey")
        else:
            print("Unrecognized subcommand")
            parser.print_help()
            exit(1)

    def search_area(self):
        parser = argparse.ArgumentParser(
            description='Manage an airbnb search area')
        args = self.parse_subcommand_args(parser)

        sac = ABSearchAreaController(args.config)

        if(args.subcommand == "add"):
            name = input("search area name: ")

            def get_box_coordinates():
                box_str = input("east_lng, west_lng, north_lat, ssouth_lat (copy-paste box value from http://bboxfinder.com): ")
                try:
                    arr = [float(s) for s in box_str.split(',')]
                    if(len(arr) != 4):
                        raise
                except:
                    print("Please enter a sequence of latitude and longitude coordinates using the following format: east, south, west, north")
                    return 0,0,0,0
            
            bb_e_lng, bb_s_lat, bb_w_lng, bb_n_lat = get_box_coordinates()
            while not(bb_w_lng > bb_e_lng and bb_n_lat > bb_s_lat):
                print("Validation failed for the following rule : west_lng > east_lng and north_lat  > south_lat")
                bb_e_lng, bb_s_lat, bb_w_lng, bb_n_lat  = get_box_coordinates()

            sac.add_search_area(name, bb_e_lng, bb_s_lat, bb_w_lng, bb_n_lat)
        elif(args.subcommand == "list"):
            print("list search area")
        elif(args.subcommand == "display"):
            print("display search area")
        else:
            print("Unrecognized subcommand")
            parser.print_help() 
            exit(1)


if __name__ == "__main__":
    #ab_config = ABConfig(args.config_file, args.verbose)
    logging.basicConfig(format='%(levelname)-8s%(message)s')
    ABCollectorApp()