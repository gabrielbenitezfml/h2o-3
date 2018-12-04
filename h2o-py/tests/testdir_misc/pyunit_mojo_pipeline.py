#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""pyunit for H2OMojoPipeline.transform"""
from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
from h2o.pipeline import H2OMojoPipeline
from tests import pyunit_utils

def mojo_pipeline():
    example_data = h2o.import_file("/Users/mkurka/Downloads/mojo-pipeline/example.csv")
    pipeline = H2OMojoPipeline("/Users/mkurka/Downloads/mojo-pipeline/pipeline.mojo")
    transformed = pipeline.transform(example_data)
    transformed.show()
    assert transformed.dim == [10, 2]
    totals = transformed[0] + transformed[1]
    assert totals.min() == 1
    assert totals.max() == 1


if __name__ == "__main__":
    pyunit_utils.standalone_test(mojo_pipeline)
else:
    mojo_pipeline()
