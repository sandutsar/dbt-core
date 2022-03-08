import json
import os
from datetime import datetime, timedelta

import yaml

from dbt.exceptions import ParsingException
import dbt.tracking
import dbt.version
from test.integration.base import DBTIntegrationTest, use_profile, AnyFloat, \
    AnyStringWith


class BaseSourcesTest(DBTIntegrationTest):
    @property
    def schema(self):
        return "sources_fresher_state_042"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'quoting': {'database': True, 'schema': True, 'identifier': True},
            'seeds': {
                'quote_columns': True,
            },
        }

    def setUp(self):
        super().setUp()
        os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE'] = 'test_run_schema'

    def tearDown(self):
        del os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE']
        super().tearDown()

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        vars_dict = {
            'test_run_schema': self.unique_schema(),
            'test_loaded_at': self.adapter.quote('updated_at'),
        }
        cmd.extend(['--vars', yaml.safe_dump(vars_dict)])
        return self.run_dbt(cmd, *args, **kwargs)


class SuccessfulSourcesTest(BaseSourcesTest):
    def setUp(self):
        super().setUp()
        self.run_dbt_with_vars(['seed'])
        self.maxDiff = None
        self._id = 101
        # this is the db initial value
        self.last_inserted_time = "2016-09-19T14:45:51+00:00"
        os.environ['DBT_ENV_CUSTOM_ENV_key'] = 'value'

    def tearDown(self):
        super().tearDown()
        del os.environ['DBT_ENV_CUSTOM_ENV_key']

    def _set_updated_at_to(self, delta):
        insert_time = datetime.utcnow() + delta
        timestr = insert_time.strftime("%Y-%m-%d %H:%M:%S")
        # favorite_color,id,first_name,email,ip_address,updated_at
        insert_id = self._id
        self._id += 1
        raw_sql = """INSERT INTO {schema}.{source}
            ({quoted_columns})
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )"""
        quoted_columns = ','.join(
            self.adapter.quote(c) for c in
            ('favorite_color', 'id', 'first_name',
             'email', 'ip_address', 'updated_at')
        )
        self.run_sql(
            raw_sql,
            kwargs={
                'schema': self.unique_schema(),
                'time': timestr,
                'id': insert_id,
                'source': self.adapter.quote('source'),
                'quoted_columns': quoted_columns,
            }
        )
        self.last_inserted_time = insert_time.strftime(
            "%Y-%m-%dT%H:%M:%S+00:00")


class TestSources(SuccessfulSourcesTest):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update({
            'macro-paths': ['macros'],
        })
        return cfg

    def _create_schemas(self):
        super()._create_schemas()
        self._create_schema_named(self.default_database,
                                  self.alternative_schema())

    def alternative_schema(self):
        return self.unique_schema() + '_other'

    def setUp(self):
        super().setUp()
        self.run_sql(
            'create table {}.dummy_table (id int)'.format(self.unique_schema())
        )
        self.run_sql(
            'create view {}.external_view as (select * from {}.dummy_table)'
            .format(self.alternative_schema(), self.unique_schema())
        )

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        vars_dict = {
            'test_run_schema': self.unique_schema(),
            'test_run_alt_schema': self.alternative_schema(),
            'test_loaded_at': self.adapter.quote('updated_at'),
        }
        cmd.extend(['--vars', yaml.safe_dump(vars_dict)])
        return self.run_dbt(cmd, *args, **kwargs)

    @use_profile('postgres')
    def test_postgres_basic_source_def(self):
        results = self.run_dbt_with_vars(['run'])
        self.assertEqual(len(results), 4)
        self.assertManyTablesEqual(
            ['source', 'descendant_model', 'nonsource_descendant'],
            ['expected_multi_source', 'multi_source_model'])
        results = self.run_dbt_with_vars(['test'])
        self.assertEqual(len(results), 6)
        print(results)

    @use_profile('postgres')
    def test_postgres_source_selector(self):
        # only one of our models explicitly depends upon a source
        results = self.run_dbt_with_vars([
            'run',
            '--models',
            'source:test_source.test_table+'
        ])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual('source', 'descendant_model')
        self.assertTableDoesNotExist('nonsource_descendant')
        self.assertTableDoesNotExist('multi_source_model')

        # do the same thing, but with tags
        results = self.run_dbt_with_vars([
            'run',
            '--models',
            'tag:my_test_source_table_tag+'
        ])
        self.assertEqual(len(results), 1)

        results = self.run_dbt_with_vars([
            'test',
            '--models',
            'source:test_source.test_table+'
        ])
        self.assertEqual(len(results), 4)

        results = self.run_dbt_with_vars([
            'test', '--models', 'tag:my_test_source_table_tag+'
        ])
        self.assertEqual(len(results), 4)

        results = self.run_dbt_with_vars([
            'test', '--models', 'tag:my_test_source_tag+'
        ])
        # test_table + other_test_table
        self.assertEqual(len(results), 6)

        results = self.run_dbt_with_vars([
            'test', '--models', 'tag:id_column'
        ])
        # all 4 id column tests
        self.assertEqual(len(results), 4)

    @use_profile('postgres')
    def test_postgres_empty_source_def(self):
        # sources themselves can never be selected, so nothing should be run
        results = self.run_dbt_with_vars([
            'run',
            '--models',
            'source:test_source.test_table'
        ])
        self.assertTableDoesNotExist('nonsource_descendant')
        self.assertTableDoesNotExist('multi_source_model')
        self.assertTableDoesNotExist('descendant_model')
        self.assertEqual(len(results), 0)

    @use_profile('postgres')
    def test_postgres_source_only_def(self):
        results = self.run_dbt_with_vars([
            'run', '--models', 'source:other_source+'
        ])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual('expected_multi_source', 'multi_source_model')
        self.assertTableDoesNotExist('nonsource_descendant')
        self.assertTableDoesNotExist('descendant_model')

        results = self.run_dbt_with_vars([
            'run', '--models', 'source:test_source+'
        ])
        self.assertEqual(len(results), 2)
        self.assertManyTablesEqual(
            ['source', 'descendant_model'],
            ['expected_multi_source', 'multi_source_model'])
        self.assertTableDoesNotExist('nonsource_descendant')

    @use_profile('postgres')
    def test_postgres_source_childrens_parents(self):
        results = self.run_dbt_with_vars([
            'run', '--models', '@source:test_source'
        ])
        self.assertEqual(len(results), 2)
        self.assertManyTablesEqual(
            ['source', 'descendant_model'],
            ['expected_multi_source', 'multi_source_model'],
        )
        self.assertTableDoesNotExist('nonsource_descendant')

    @use_profile('postgres')
    def test_postgres_run_operation_source(self):
        kwargs = '{"source_name": "test_source", "table_name": "test_table"}'
        self.run_dbt_with_vars([
            'run-operation', 'vacuum_source', '--args', kwargs
        ])


class TestSourceFreshness(SuccessfulSourcesTest):

    def _assert_freshness_results(self, path, state):
        self.assertTrue(os.path.exists(path))
        with open(path) as fp:
            data = json.load(fp)

        assert set(data) == {'metadata', 'results', 'elapsed_time'}
        assert 'generated_at' in data['metadata']
        assert isinstance(data['elapsed_time'], float)
        self.assertBetween(data['metadata']['generated_at'],
                           self.freshness_start_time)
        assert data['metadata']['dbt_schema_version'] == 'https://schemas.getdbt.com/dbt/sources/v3.json'
        assert data['metadata']['dbt_version'] == dbt.version.__version__
        assert data['metadata']['invocation_id'] == dbt.tracking.active_user.invocation_id
        key = 'key'
        if os.name == 'nt':
            key = key.upper()
        assert data['metadata']['env'] == {key: 'value'}

        last_inserted_time = self.last_inserted_time

        self.assertEqual(len(data['results']), 1)

        self.assertEqual(data['results'], [
            {
                'unique_id': 'source.test.test_source.test_table',
                'max_loaded_at': last_inserted_time,
                'snapshotted_at': AnyStringWith(),
                'max_loaded_at_time_ago_in_s': AnyFloat(),
                'status': state,
                'criteria': {
                    'filter': None,
                    'warn_after': {'count': 10, 'period': 'hour'},
                    'error_after': {'count': 18, 'period': 'hour'},
                },
                'adapter_response': {},
                'thread_id': AnyStringWith('Thread-'),
                'execution_time': AnyFloat(),
                'timing': [
                    {
                        'name': 'compile',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    },
                    {
                        'name': 'execute',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    }
                ]
            }
        ])

     # TODO: move the sources.json to a previous state folder
    def _run_source_freshness(self):
        # test_source.test_table should have a loaded_at field of `updated_at`
        # and a freshness of warn_after: 10 hours, error_after: 18 hours
        # by default, our data set is way out of date!
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/error_source.json'],
            expect_pass=False
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'error')
        self._assert_freshness_results('target/error_source.json', 'error')

        self._set_updated_at_to(timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/warn_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'warn')
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/pass_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'pass')
        self._assert_freshness_results('target/pass_source.json', 'pass')

    @use_profile('postgres')
    def test_postgres_source_freshness(self):
        self._run_source_freshness()

# class TestRunResultsState(DBTIntegrationTest):
#     @property
#     def schema(self):
#         return "source_fresher_state_042"

#     @property
#     def models(self):
#         return "models"

#     @property
#     def project_config(self):
#         return {
#             'config-version': 2,
#             'macro-paths': ['macros'],
#             'seeds': {
#                 'test': {
#                     'quote_columns': True,
#                 }
#             }
#         }

#     def _symlink_test_folders(self):
#         # dbt's normal symlink behavior breaks this test. Copy the files
#         # so we can freely modify them.
#         for entry in os.listdir(self.test_original_source_path):
#             src = os.path.join(self.test_original_source_path, entry)
#             tst = os.path.join(self.test_root_dir, entry)
#             if entry in {'models', 'seeds', 'macros'}:
#                 shutil.copytree(src, tst)
#             elif os.path.isdir(entry) or entry.endswith('.sql'):
#                 os.symlink(src, tst)

#     def copy_state(self):
#         assert not os.path.exists('state')
#         os.makedirs('state')
#         shutil.copyfile('target/manifest.json', 'state/manifest.json')
#         shutil.copyfile('target/run_results.json', 'state/run_results.json')
#         shutil.copyfile('target/sources.json', 'state/sources.json')

#     # TODO: upload a seed file to serve as the source and run source freshness on it
#    
#     def setUp(self):
#         super().setUp()
#         self.run_dbt(['source', 'freshness'])
#         self.copy_state()
    
#     def rebuild_run_dbt(self, expect_pass=True):
#         shutil.rmtree('./state')
#         self.run_dbt(['source', 'freshness'], expect_pass=expect_pass)
#         self.copy_state()

#     @use_profile('postgres')
#     def test_postgres_run_source_fresher_state(self):
#         results = self.run_dbt(['run', '--select', 'source_status:fresher+', '--state', './state'], expect_pass=True)
#         assert len(results) == 0

#         # clear state and rerun upstream view model to test + operator
#         shutil.rmtree('./state')
#         self.run_dbt(['run', '--select', 'view_model'], expect_pass=True)
#         self.copy_state()
#         results = self.run_dbt(['run', '--select', 'result:success+', '--state', './state'], expect_pass=True)
#         assert len(results) == 2
#         assert results[0].node.name == 'view_model'
#         assert results[1].node.name == 'table_model'

    #     # check we are starting from a place with 0 errors
    #     results = self.run_dbt(['run', '--select', 'result:error', '--state', './state'])
    #     assert len(results) == 0
        
    #     # force an error in the view model to test error and skipped states
    #     with open('models/view_model.sql') as fp:
    #         fp.readline()
    #         newline = fp.newlines

    #     with open('models/view_model.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select * from forced_error")
    #         fp.write(newline)
        
    #     shutil.rmtree('./state')
    #     self.run_dbt(['run'], expect_pass=False)
    #     self.copy_state()

    #     # test single result selector on error
    #     results = self.run_dbt(['run', '--select', 'result:error', '--state', './state'], expect_pass=False)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'view_model'
        
    #     # test + operator selection on error
    #     results = self.run_dbt(['run', '--select', 'result:error+', '--state', './state'], expect_pass=False)
    #     assert len(results) == 2
    #     assert results[0].node.name == 'view_model'
    #     assert results[1].node.name == 'table_model'

    #     # single result selector on skipped. Expect this to pass becase underlying view already defined above
    #     results = self.run_dbt(['run', '--select', 'result:skipped', '--state', './state'], expect_pass=True)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'table_model'

    #     # add a downstream model that depends on table_model for skipped+ selector
    #     with open('models/table_model_downstream.sql', 'w') as fp:
    #         fp.write("select * from {{ref('table_model')}}")
        
    #     shutil.rmtree('./state')
    #     self.run_dbt(['run'], expect_pass=False)
    #     self.copy_state()

    #     results = self.run_dbt(['run', '--select', 'result:skipped+', '--state', './state'], expect_pass=True)
    #     assert len(results) == 2
    #     assert results[0].node.name == 'table_model'
    #     assert results[1].node.name == 'table_model_downstream'

    # @use_profile('postgres')
    # def test_postgres_build_run_results_state(self):
    #     results = self.run_dbt(['build', '--select', 'result:error', '--state', './state'])
    #     assert len(results) == 0

    #     with open('models/view_model.sql') as fp:
    #         fp.readline()
    #         newline = fp.newlines

    #     with open('models/view_model.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select * from forced_error")
    #         fp.write(newline)
        
    #     self.rebuild_run_dbt(expect_pass=False)

    #     results = self.run_dbt(['build', '--select', 'result:error', '--state', './state'], expect_pass=False)
    #     assert len(results) == 3
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'view_model', 'not_null_view_model_id','unique_view_model_id'}

    #     results = self.run_dbt(['ls', '--select', 'result:error', '--state', './state'])
    #     assert len(results) == 3
    #     assert set(results) == {'test.view_model', 'test.not_null_view_model_id', 'test.unique_view_model_id'}

    #     results = self.run_dbt(['build', '--select', 'result:error+', '--state', './state'], expect_pass=False)
    #     assert len(results) == 4
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'table_model','view_model', 'not_null_view_model_id','unique_view_model_id'}

    #     results = self.run_dbt(['ls', '--select', 'result:error+', '--state', './state'])
    #     assert len(results) == 6 # includes exposure
    #     assert set(results) == {'test.table_model', 'test.view_model', 'test.ephemeral_model', 'test.not_null_view_model_id', 'test.unique_view_model_id', 'exposure:test.my_exposure'}

    #     # test failure on build tests
    #     # fail the unique test
    #     with open('models/view_model.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select 1 as id union all select 1 as id")
    #         fp.write(newline)
        
    #     self.rebuild_run_dbt(expect_pass=False)

    #     results = self.run_dbt(['build', '--select', 'result:fail', '--state', './state'], expect_pass=False)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'unique_view_model_id'

    #     results = self.run_dbt(['ls', '--select', 'result:fail', '--state', './state'])
    #     assert len(results) == 1
    #     assert results[0] == 'test.unique_view_model_id'

    #     results = self.run_dbt(['build', '--select', 'result:fail+', '--state', './state'], expect_pass=False)
    #     assert len(results) == 2
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'table_model', 'unique_view_model_id'}

    #     results = self.run_dbt(['ls', '--select', 'result:fail+', '--state', './state'])
    #     assert len(results) == 2
    #     assert set(results) == {'test.table_model', 'test.unique_view_model_id'}

    #     # change the unique test severity from error to warn and reuse the same view_model.sql changes above
    #     f = open('models/schema.yml', 'r')
    #     filedata = f.read()
    #     f.close()
    #     newdata = filedata.replace('error','warn')
    #     f = open('models/schema.yml', 'w')
    #     f.write(newdata)
    #     f.close()

    #     self.rebuild_run_dbt(expect_pass=True)

    #     results = self.run_dbt(['build', '--select', 'result:warn', '--state', './state'], expect_pass=True)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'unique_view_model_id'

    #     results = self.run_dbt(['ls', '--select', 'result:warn', '--state', './state'])
    #     assert len(results) == 1
    #     assert results[0] == 'test.unique_view_model_id'

    #     results = self.run_dbt(['build', '--select', 'result:warn+', '--state', './state'], expect_pass=True)
    #     assert len(results) == 2 # includes table_model to be run
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'table_model', 'unique_view_model_id'}

    #     results = self.run_dbt(['ls', '--select', 'result:warn+', '--state', './state'])
    #     assert len(results) == 2
    #     assert set(results) == {'test.table_model', 'test.unique_view_model_id'}

    
    
    # @use_profile('postgres')
    # def test_postgres_test_run_results_state(self):
    #     # run passed nodes
    #     results = self.run_dbt(['test', '--select', 'result:pass', '--state', './state'], expect_pass=True)
    #     assert len(results) == 2
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'unique_view_model_id', 'not_null_view_model_id'}
        
    #     # run passed nodes with + operator
    #     results = self.run_dbt(['test', '--select', 'result:pass+', '--state', './state'], expect_pass=True)
    #     assert len(results) == 2
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'unique_view_model_id', 'not_null_view_model_id'}

    #     # update view model to generate a failure case
    #     os.remove('./models/view_model.sql')
    #     with open('models/view_model.sql', 'w') as fp:
    #         fp.write("select 1 as id union all select 1 as id")
        
    #     self.rebuild_run_dbt(expect_pass=False)

    #     # test with failure selector
    #     results = self.run_dbt(['test', '--select', 'result:fail', '--state', './state'], expect_pass=False)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'unique_view_model_id'

    #     # test with failure selector and + operator
    #     results = self.run_dbt(['test', '--select', 'result:fail+', '--state', './state'], expect_pass=False)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'unique_view_model_id'

    #     # change the unique test severity from error to warn and reuse the same view_model.sql changes above
    #     with open('models/schema.yml', 'r+') as f:
    #         filedata = f.read()
    #         newdata = filedata.replace('error','warn')
    #         f.seek(0)
    #         f.write(newdata)
    #         f.truncate()
        
    #     # rebuild - expect_pass = True because we changed the error to a warning this time around
    #     self.rebuild_run_dbt(expect_pass=True)

    #     # test with warn selector
    #     results = self.run_dbt(['test', '--select', 'result:warn', '--state', './state'], expect_pass=True)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'unique_view_model_id'

    #     # test with warn selector and + operator
    #     results = self.run_dbt(['test', '--select', 'result:warn+', '--state', './state'], expect_pass=True)
    #     assert len(results) == 1
    #     assert results[0].node.name == 'unique_view_model_id'


    # @use_profile('postgres')
    # def test_postgres_concurrent_selectors_run_run_results_state(self):
    #     results = self.run_dbt(['run', '--select', 'state:modified+', 'result:error+', '--state', './state'])
    #     assert len(results) == 0

    #     # force an error on a dbt model
    #     with open('models/view_model.sql') as fp:
    #         fp.readline()
    #         newline = fp.newlines

    #     with open('models/view_model.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select * from forced_error")
    #         fp.write(newline)
        
    #     shutil.rmtree('./state')
    #     self.run_dbt(['run'], expect_pass=False)
    #     self.copy_state()

    #     # modify another dbt model
    #     with open('models/table_model_modified_example.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select * from forced_error")
    #         fp.write(newline)
        
    #     results = self.run_dbt(['run', '--select', 'state:modified+', 'result:error+', '--state', './state'], expect_pass=False)
    #     assert len(results) == 3
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'view_model', 'table_model_modified_example', 'table_model'}
    

    # @use_profile('postgres')
    # def test_postgres_concurrent_selectors_test_run_results_state(self):
    #     # create failure test case for result:fail selector
    #     os.remove('./models/view_model.sql')
    #     with open('./models/view_model.sql', 'w') as f:
    #         f.write('select 1 as id union all select 1 as id union all select null as id')

    #     # run dbt build again to trigger test errors
    #     self.rebuild_run_dbt(expect_pass=False)
        
    #     # get the failures from 
    #     results = self.run_dbt(['test', '--select', 'result:fail', '--exclude', 'not_null_view_model_id', '--state', './state'], expect_pass=False)
    #     assert len(results) == 1
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'unique_view_model_id'}
        
        
    # @use_profile('postgres')
    # def test_postgres_concurrent_selectors_build_run_results_state(self):
    #     results = self.run_dbt(['build', '--select', 'state:modified+', 'result:error+', '--state', './state'])
    #     assert len(results) == 0

    #     # force an error on a dbt model
    #     with open('models/view_model.sql') as fp:
    #         fp.readline()
    #         newline = fp.newlines

    #     with open('models/view_model.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select * from forced_error")
    #         fp.write(newline)
        
    #     self.rebuild_run_dbt(expect_pass=False)

    #     # modify another dbt model
    #     with open('models/table_model_modified_example.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select * from forced_error")
    #         fp.write(newline)
        
    #     results = self.run_dbt(['build', '--select', 'state:modified+', 'result:error+', '--state', './state'], expect_pass=False)
    #     assert len(results) == 5
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'table_model_modified_example', 'view_model', 'table_model', 'not_null_view_model_id', 'unique_view_model_id'}
        
    #     # create failure test case for result:fail selector
    #     os.remove('./models/view_model.sql')
    #     with open('./models/view_model.sql', 'w') as f:
    #         f.write('select 1 as id union all select 1 as id')

    #     # create error model case for result:error selector
    #     with open('./models/error_model.sql', 'w') as f:
    #         f.write('select 1 as id from not_exists')
        
    #     # create something downstream from the error model to rerun
    #     with open('./models/downstream_of_error_model.sql', 'w') as f:
    #         f.write('select * from {{ ref("error_model") }} )')
        
    #     # regenerate build state
    #     self.rebuild_run_dbt(expect_pass=False)

    #     # modify model again to trigger the state:modified selector 
    #     with open('models/table_model_modified_example.sql', 'w') as fp:
    #         fp.write(newline)
    #         fp.write("select * from forced_another_error")
    #         fp.write(newline)
        
    #     results = self.run_dbt(['build', '--select', 'state:modified+', 'result:error+', 'result:fail+', '--state', './state'], expect_pass=False)
    #     assert len(results) == 5
    #     nodes = set([elem.node.name for elem in results])
    #     assert nodes == {'error_model', 'downstream_of_error_model', 'table_model_modified_example', 'table_model', 'unique_view_model_id'}
