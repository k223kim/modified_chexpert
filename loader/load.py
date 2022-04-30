"""Define report loader class."""
import re
import bioc
import pandas as pd
from negbio.pipeline import text2bioc, ssplit, section_split

from constants import *


class Loader(object):
    """Report impression loader."""
    def __init__(self, reports_path, extract_impression=False):
        self.reports_path = reports_path
        self.extract_impression = extract_impression
        self.punctuation_spacer = str.maketrans({key: f"{key} "
                                                 for key in ".,"})
        self.splitter = ssplit.NegBioSSplitter(newline=False)
        
    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def load(self):
        """Load and clean the reports."""
        list_collection = []
#         collection = bioc.BioCCollection()
        reports = pd.read_csv(self.reports_path,
                              header=None,
                              names=[REPORTS])[REPORTS].tolist()
        list_reports = list(self.chunks(reports, 1000))

        for i, inst_report in enumerate(list_reports):
            collection = bioc.BioCCollection()
            for j, report in enumerate(inst_report):
                clean_report = self.clean(report)
                document = text2bioc.text2document(str(j), clean_report)

                if self.extract_impression:
                    document = section_split.split_document(document)
                    self.extract_impression_from_passages(document)

                split_document = self.splitter.split_doc(document)

                assert len(split_document.passages) == 1,\
                    ('Each document must have a single passage, ' +
                     'the Impression section.')

                collection.add_document(split_document)
            list_collection.append(collection)

        self.reports = list_reports
#         self.collection = collection
        self.collection = list_collection

    def extract_impression_from_passages(self, document):
        """Extract the Impression section from a Bioc Document."""
        impression_passages = []
        for i, passage in enumerate(document.passages):
            if 'title' in passage.infons:
                if passage.infons['title'] == 'impression':
                    next_passage = document.passages[i+1]
                    assert 'title' not in next_passage.infons,\
                        "Document contains empty impression section."
                    impression_passages.append(next_passage)

        assert len(impression_passages) <= 1,\
            (f"The document contains {len(document.passages)} impression " +
             "passages.")

        assert len(impression_passages) >= 1,\
            "The document contains no explicit impression passage."

        document.passages = impression_passages

    def clean(self, report):
        """Clean the report text."""
        lower_report = report.lower()
        # Change `and/or` to `or`.
        corrected_report = re.sub('and/or',
                                  'or',
                                  lower_report)
        # Change any `XXX/YYY` to `XXX or YYY`.
        corrected_report = re.sub('(?<=[a-zA-Z])/(?=[a-zA-Z])',
                                  ' or ',
                                  corrected_report)
        # Clean double periods
        clean_report = corrected_report.replace("..", ".")
        # Insert space after commas and periods.
        clean_report = clean_report.translate(self.punctuation_spacer)
        # Convert any multi white spaces to single white spaces.
        clean_report = ' '.join(clean_report.split())
        # Remove empty sentences
        clean_report = re.sub(r'\.\s+\.', '.', clean_report)

        return clean_report
