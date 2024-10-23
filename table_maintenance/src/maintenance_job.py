import argparse
from maintenance.delta_table_maintenance import TableMaintenance


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Perform maintenance on Delta tables.")
    parser.add_argument(
        "-t", "--tables",
        help="Table names in the format catalog.schema.table, separated by commas",
        required=True
    )
    parser.add_argument(
        "-rh", "--retention_hours",
        help="Retention hours for vacuum",
        default="720"
    )
    parser.add_argument(
        "--optimize_where",
        help="Partition predicate for Optimize",
        default=None,
        type=change_empty_args
    )
    parser.add_argument(
        "--delete_where",
        help="Partition predicate for Delete Purge",
        default=None,
        type=change_empty_args
    )
    return parser.parse_args()

def change_empty_args(val):
    if val == '':
        return None
    return val

def main():
    args = parse_arguments()

    # Create an instance of TableMaintenance
    tbl_maint = TableMaintenance()

    # Set the table names
    tables = args.tables.split(",")

    # Perform maintenance on each table
    for table in tables:
        tbl_maint.set_table(table)
        tbl_maint.delete_purge(where_clause=args.delete_where)
        print(f"Delete + Purged table {table} successfully")
        tbl_maint.optimize(where=args.optimize_where)
        print(f"Optimized table {table} successfully")
        tbl_maint.vacuum(retention=args.retention_hours)
        print(f"Table {table} maintenance completed successfully")

if __name__ == "__main__":
    main()