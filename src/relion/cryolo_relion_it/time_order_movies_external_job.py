#!/usr/bin/env python3
"""
External job for fitting FSC curves and finding the value at which they cross 0.5
"""

import argparse
import json
import os
import os.path
import pathlib

import gemmi

RELION_JOB_FAILURE_FILENAME = "RELION_JOB_EXIT_FAILURE"
RELION_JOB_SUCCESS_FILENAME = "RELION_JOB_EXIT_SUCCESS"


def run_job(project_dir, out_dir, in_mics_file, args_list):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )

    in_mics = gemmi.cif.read_file(os.path.join(project_dir, in_mics_file))
    proj_dir = pathlib.Path(project_dir)
    data_as_dict = json.loads(in_mics.as_json())["data_movies"][
        "_rlnmicrographmoviename"
    ]
    data = [
        (d1, d2)
        for d1, d2 in zip(
            data_as_dict["_rlnmicrographmoviename"], data_as_dict["_rlnOpticsGroup"]
        )
    ]
    if proj_dir.is_dir():
        existing_star = os.path.join(project_dir, out_dir, "movies.star")
        known_as_dict = json.loads(existing_star.as_json())["data_movies"]
        known_mics = known_as_dict["_rlnmicrographmoviename"]
        known_mics_og = known_as_dict["_rlnOpticsGroup"]
    else:
        known_mics = []
        known_mics_og = []
    time_ordered = sorted(
        [d for d in data if d[0] not in known_mics],
        key=lambda p: (proj_dir / p[0]).stat().st_ctime,
    )
    all_mics = time_ordered + [(d1, d2) for d1, d2 in zip(known_mics, known_mics_og)]
    out_star = gemmi.cif.Document()
    data_movies_block = out_star.add_new_block("data_movies")
    loop = data_movies_block.init_loop(
        "", ["_rlnMicrographMovieName", "_rlnOpticsGroup"]
    )
    for m, g in all_mics:
        loop.add_row([m, g])
    out_star.write_file(os.path.join(project_dir, out_dir, "movies.star"))

    return


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out_dir",
        dest="out_dir",
        help="Directory for the movie time ordering External job",
    )
    parser.add_argument(
        "--i",
        dest="star_mics",
        help="Input star file name from Import job",
    )
    parser.add_argument(
        "--pipeline_control", help="Directory for pipeline control files"
    )
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    os.makedirs(known_args.out_dir, exist_ok=True)
    os.chdir(known_args.out_dir)
    if os.path.isfile(RELION_JOB_FAILURE_FILENAME):
        print(" fsc_fitting_external_job: Removing previous failure indicator file")
        os.remove(RELION_JOB_FAILURE_FILENAME)
    if os.path.isfile(RELION_JOB_SUCCESS_FILENAME):
        print(" fsc_fitting_external_job: Removing previous success indicator file")
        os.remove(RELION_JOB_SUCCESS_FILENAME)
    try:
        os.chdir("../..")
        run_job(project_dir, known_args.out_dir, known_args.star_mics, other_args)
    except Exception:
        os.chdir(known_args.out_dir)
        open(RELION_JOB_FAILURE_FILENAME, "w").close()
        raise
    else:
        os.chdir(known_args.out_dir)
        open(RELION_JOB_SUCCESS_FILENAME, "w").close()


if __name__ == "__main__":
    main()
