"""Entry-point script to label radiology reports."""
import pandas as pd
import numpy as np
from args import ArgParser
from loader import Loader
from stages import Extractor, Classifier, Aggregator
from constants import *
from tqdm import tqdm


def write(reports, labels, output_path, verbose=False):
    """Write labeled reports to specified path."""
    labeled_reports = pd.DataFrame({REPORTS: reports})
    for index, category in enumerate(CATEGORIES):
        labeled_reports[category] = labels[:, index]

    if verbose:
        print(f"Writing reports and labels to {output_path}.")
    labeled_reports[[REPORTS] + CATEGORIES].to_csv(output_path,
                                                   index=False)


def label(args):
    """Label the provided report(s)."""

    loader = Loader(args.reports_path, args.extract_impression)

    extractor = Extractor(args.mention_phrases_dir,
                          args.unmention_phrases_dir,
                          verbose=args.verbose)
    classifier = Classifier(args.pre_negation_uncertainty_path,
                            args.negation_path,
                            args.post_negation_uncertainty_path,
                            verbose=args.verbose)
    aggregator = Aggregator(CATEGORIES,
                            verbose=args.verbose)

    # Load reports in place.
    loader.load()
    # Extract observation mentions in place.
    number = 0
    for inst_loader, inst_reports in zip(loader.collection, loader.reports):
        number += 1
        extractor.extract(inst_loader)
        # Classify mentions in place.
        classifier.classify(inst_loader)
        # Aggregate mentions to obtain one set of labels for each report.
        labels = aggregator.aggregate(inst_loader)
        output_path = args.output_path
        temp_output_path = str(output_path).split(".")[0]+"_"+str(number)+".csv"
        write(inst_reports, labels, temp_output_path, args.verbose)



if __name__ == "__main__":
    parser = ArgParser()
    label(parser.parse_args())
