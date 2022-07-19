# Copyright 2018-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License'). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the 'license' file accompanying this file. This file is
# distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
from __future__ import absolute_import

from glob import glob
import os

import setuptools


def read(file_name):
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()


packages = setuptools.find_packages(where="src", exclude=("test",))

setuptools.setup(
    name="pathway",
    version="0.0",
    description="Open source library for creating containers to run on Amazon SageMaker.",
    packages=packages,
    package_dir={"pathway": "src/pathway"},
    py_modules=[os.path.splitext(os.path.basename(path))[0] for path in glob("src/*.py")],
    author="Zhankui Lu",
    install_requires=[
        'typing_extensions',
        'cloudpickle',
        'boto3'
    ],
    extra_requires={
        'client': [
            'sagemaker'
        ]
    },
    entry_points={
        "console_scripts": [
            "pathway-runtime=pathway.runtime.cli.cli:main",
        ]
    },
)
