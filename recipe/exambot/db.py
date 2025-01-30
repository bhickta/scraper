from sqlalchemy import create_engine, Table, MetaData, and_, or_, text
from sqlalchemy.orm import sessionmaker

class GenericDatabase:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def get_table(self, table_name):
        return Table(table_name, self.metadata, autoload_with=self.engine)


    def query_with_filters(self, table_name, filters=None, conjunction="and", limit=None, offset=None):
        table = self.get_table(table_name)
        query = self.session.query(table)

        if filters:
            filter_clauses = []
            
            # If filters are passed as a list of raw SQL-like conditions (e.g., ["page_no > 387"])
            if isinstance(filters, list):
                for condition in filters:
                    # Using SQLAlchemy's `text()` for raw SQL condition
                    filter_clauses.append(text(condition))
            else:
                # If filters are passed as a dictionary
                filter_clauses = [
                    getattr(table.c, column) == value for column, value in filters.items()
                ]
            
            # Combine the filter clauses with the conjunction
            if conjunction == "and":
                query = query.filter(*filter_clauses)
            elif conjunction == "or":
                query = query.filter(or_(*filter_clauses))  # Use `or_` for "or" conjunction

        # Apply limit and offset if provided
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        return query.all()

    def insert(self, table_name, data):
        table = self.get_table(table_name)
        with self.engine.begin() as conn:
            conn.execute(table.insert(), data)

    def update(self, table_name, filters, data):
        table = self.get_table(table_name)
        filter_clauses = [getattr(table.c, col) == val for col, val in filters.items()]
        with self.engine.begin() as conn:
            result = conn.execute(
                table.update().where(and_(*filter_clauses)).values(**data)
            )
        return result.rowcount

    def delete(self, table_name, filters):
        table = self.get_table(table_name)
        filter_clauses = [getattr(table.c, col) == val for col, val in filters.items()]
        with self.engine.begin() as conn:
            result = conn.execute(
                table.delete().where(and_(*filter_clauses))
            )
        return result.rowcount