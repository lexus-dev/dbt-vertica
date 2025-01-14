# Copyright (c) [2018-2023]  Micro Focus or one of its affiliates.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pytest
from dbt.tests.adapter.utils.base_array_utils import BaseArrayUtils
from dbt.tests.adapter.utils.fixture_array_construct import (
    models__array_construct_actual_sql,
    models__array_construct_expected_sql,
)


class BaseArrayConstruct(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_construct_actual_sql,
            "expected.sql": models__array_construct_expected_sql,
        }


class TestArrayConstruct(BaseArrayConstruct):
    pass
