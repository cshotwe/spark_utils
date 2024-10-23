import pytest
import argparse
from unittest.mock import patch
from maintenance_job import parse_arguments, main, change_empty_args
from unittest.mock import patch, call

def test_change_empty_args():
    assert change_empty_args('') is None
    assert change_empty_args('value') == 'value'

@patch('argparse.ArgumentParser.parse_args')
def test_parse_arguments(mock_parse_args):
    mock_parse_args.return_value = argparse.Namespace(
        tables="catalog.schema.table1,catalog.schema.table2",
        retention_hours="720",
        optimize_where=None,
        delete_where=None
    )
    args = parse_arguments()
    assert args.tables == "catalog.schema.table1,catalog.schema.table2"
    assert args.retention_hours == "720"
    assert args.optimize_where is None
    assert args.delete_where is None

@patch('maintenance_job.TableMaintenance')
@patch('maintenance_job.parse_arguments')
def test_main(mock_parse_arguments, MockTableMaintenance):
    mock_parse_arguments.return_value = argparse.Namespace(
        tables="catalog.schema.table1,catalog.schema.table2",
        retention_hours="720",
        optimize_where=None,
        delete_where=None
    )
    mock_tbl_maint = MockTableMaintenance.return_value

    main()

    assert mock_tbl_maint.set_table.call_count == 2
    assert mock_tbl_maint.optimize.call_count == 2
    assert mock_tbl_maint.delete_purge.call_count == 2
    assert mock_tbl_maint.vacuum.call_count == 2

    mock_tbl_maint.set_table.assert_any_call("catalog.schema.table1")
    mock_tbl_maint.set_table.assert_any_call("catalog.schema.table2")
    mock_tbl_maint.optimize.assert_any_call(where=None)
    mock_tbl_maint.delete_purge.assert_any_call(where_clause=None)
    mock_tbl_maint.vacuum.assert_any_call(retention="720")

def test_integration_main():
    with patch('maintenance_job.TableMaintenance') as MockTableMaintenance:
        with patch('argparse.ArgumentParser.parse_args') as mock_parse_args:
            mock_parse_args.return_value = argparse.Namespace(
                tables="catalog.schema.table1,catalog.schema.table2",
                retention_hours="720",
                optimize_where=None,
                delete_where=None
            )
            mock_tbl_maint = MockTableMaintenance.return_value

            main()

            expected_calls = [
                call.set_table("catalog.schema.table1"),
                call.delete_purge(where_clause=None),
                call.optimize(where=None),
                call.vacuum(retention="720"),
                call.set_table("catalog.schema.table2"),
                call.delete_purge(where_clause=None),
                call.optimize(where=None),
                call.vacuum(retention="720")
            ]

            mock_tbl_maint.assert_has_calls(expected_calls, any_order=False)