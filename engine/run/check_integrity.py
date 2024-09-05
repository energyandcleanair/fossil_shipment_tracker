from argparse import ArgumentParser
import integrity
import base

import datetime as dt


def update(check_names: list[str] | None = None):

    if check_names is None or len(check_names) == 0:
        integrity.check()
        return
    else:
        filtered_steps = [step for step in integrity.IntegrityStep if step.name in check_names]
        integrity.check(steps=filtered_steps)
        return


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("--checks", nargs="*", type=str, default=None)

    args = parser.parse_args()

    check_names = args.checks

    print("=== Using %s environment ===" % (base.db.environment,))
    update(check_names=check_names)
