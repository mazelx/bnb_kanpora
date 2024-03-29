#!/usr/bin/python3
# ============================================================================
# Airbnb web site scraper, for analysis of Airbnb listings
# Xavier Mazellier, Tom Slee, 2013--2015.
# WIP
# ============================================================================
import logging
import argparse
import sys
from bnb_kanpora.config import Config

from bnb_kanpora.controllers import DatabaseController, SearchAreaController, SearchSurveyController
from bnb_kanpora.views import ABSearchAreaViewer, ABSurveyViewer
from bnb_kanpora.utils import GeoBox

SCRIPT_VERSION_NUMBER = "0.1.0"
 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ABCollectorApp:

    def __init__(self):
        """
        Read and parse command-line arguments]
        """
        parser = argparse.ArgumentParser(   
            description='Manage a database of Airbnb listings.',
            usage='''%(prog)s <command> [<args>]
        
            Available commands:

                survey [run|delete|list|run_extra|export]
                search_area [add|delete|list]
                db [check]

            Optional args:
                -v | --verbose
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
                            metavar="config_file", action="store", default="./app.config",
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
            logger.setLevel(logging.DEBUG)
        if(args.config_file):
            print(f"with config {args.config_file}")
        
        return args

    def db(self):
        parser = argparse.ArgumentParser(
            description='Manage an airbnb survey')
        args = self.parse_subcommand_args(parser)
        
        config = Config(args.config_file)
        db = DatabaseController(config)

        if(args.subcommand == "check"):            
            try:
                db.db_check_connection()
                print("Connection OK")
            except Exception as e:
                print("Something went wrong with the DB connection, please check your config file")
                print(e)
        else:
            print("Unrecognized subcommand")
            parser.print_help()
            exit(1)

    def survey(self):
        parser = argparse.ArgumentParser(description='Manage an airbnb survey')
        args = self.parse_subcommand_args(parser)

        config = Config(args.config_file)
        survey_controller = SearchSurveyController(config)
        survey_viewer = ABSurveyViewer()
        search_area_viewer = ABSearchAreaViewer()

        if(args.subcommand == "delete"):
            survey_viewer.print_surveys()
            survey_id = input("survey_id : ")
            question = "Are you sure you want to delete listings for survey {}? [y/N] ".format(survey_id)
            sys.stdout.write(question)
            choice = input().lower()
            if choice != "y":
                print("Cancelling the request.")
                return
            survey_controller.delete(survey_id)

        elif(args.subcommand == "list"):
            survey_viewer.print_surveys()
            
        elif(args.subcommand == "run"):
            search_area_viewer.print_search_areas()
            search_area_id = input("search_area_id : ")
            survey_id = survey_controller.add(search_area_id)
            results = survey_controller.run(survey_id)
            logger.info(f"Finished survey {survey_id} (search area {search_area_id}) : {results.total_nb_rooms} parsed, {results.total_nb_saved} saved, {results.total_nb_rooms_expected} expected")
        
        elif(args.subcommand == "run_extra"):
            print("run extra information search for survey")
        
        elif(args.subcommand == "export"):
            survey_viewer.print_surveys()
            survey_id = input("survey_ids (separated by ',') : ")
            survey_controller.export(survey_id.split(','))
        else:
            print("Unrecognized subcommand")
            parser.print_help()
            exit(1)

    def search_area(self):
        parser = argparse.ArgumentParser(
            description='Manage an airbnb search area')
        args = self.parse_subcommand_args(parser)

        config = Config(args.config_file)
        sac = SearchAreaController(config)
        search_area_viewer = ABSearchAreaViewer()

        if(args.subcommand == "add"):
            name = input("search area name: ")

            def get_box_coordinates():
                box_str = input("south, west, north, east (copy-paste coordinates after # in URL from http://bboxfinder.com): ")
                try:
                    arr = [float(s) for s in box_str.split(',')]
                    if(len(arr) != 4):
                        raise
                    return tuple(arr)
                except:
                    return 0,0,0,0
            
            s_lat, w_lng, n_lat, e_lng = get_box_coordinates()
            while not(e_lng > w_lng and n_lat > s_lat):
                print("Validation failed for the following rule : west_lng > east_lng and north_lat  > south_lat")
                s_lat, w_lng, n_lat, e_lng  = get_box_coordinates()

            sac.add(name, GeoBox(s_lat=s_lat, w_lng=w_lng, n_lat=n_lat, e_lng=e_lng))
        elif(args.subcommand == "list"):
            search_area_viewer.print_search_areas()
        elif(args.subcommand == "delete"):
            search_area_viewer.print_search_areas()
            search_area_id = input("search area id to delete: ")
            sac.delete(search_area_id)

        else:
            print("Unrecognized subcommand")
            parser.print_help() 
            exit(1)


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)-8s%(message)s')
    ABCollectorApp()