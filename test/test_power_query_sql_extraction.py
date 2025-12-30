import pandas as pd
from pbixray.meta.metadata_query import MetadataQuery


class FakeHandler:
    def __init__(self):
        self.queries = []

    def execute_query(self, sql):
        self.queries.append(sql)
        # Return empty for most queries
        if 'FROM partition' in sql and 'p.Type = 4' in sql:
            return pd.DataFrame([
                {'TableName': 'TestTable', 'Expression': "let Source = Value.NativeQuery(Connector, \"SELECT id, name FROM users WHERE active = 1\") in Source"},
                {'TableName': 'TestTable2', 'Expression': "let Source = Value.NativeQuery(Connector, \"WITH cte AS (SELECT id FROM users) SELECT * FROM cte\") in Source"},
                {'TableName': 'TestTable3', 'Expression': "let Source = Value.NativeQuery(Connector, \"SET NOCOUNT ON; SELECT id FROM users\") in Source"},
                {'TableName': 'TestTable4', 'Expression': "let Source = Value.NativeQuery(Connector, \"SELECT \"\"custom_id\"\", value FROM table\") in Source"},
                {'TableName': 'TestTable5', 'Expression': (
                    "let\n    Code = \"\nSELECT DISTINCT \"\"Customer_id\"\" id\nFROM myTable\nWHERE id > 0\"\nin Source"
                )}
            ])
        return pd.DataFrame()

    def close_connection(self):
        pass


def test_sql_extraction_from_expression():
    handler = FakeHandler()
    mq = MetadataQuery(handler)
    df = mq.m_df
    assert 'SqlQuery' in df.columns
    assert df.loc[0, 'SqlQuery'].upper().startswith('SELECT ID, NAME'), 'SQL extraction failed for SELECT'
    assert df.loc[1, 'SqlQuery'].upper().startswith('WITH CTE'), 'SQL extraction failed for WITH CTE'
    assert df.loc[2, 'SqlQuery'].upper().startswith('SET NOCOUNT'), 'SQL extraction failed for SET prefix'
    assert '"custom_id"' in df.loc[3, 'SqlQuery'], 'Doubled-quote identifier extraction failed'
    # Multiline expression starting with newline; expect SELECT DISTINCT and quoted Cyrillic identifier preserved
    assert df.loc[4, 'SqlQuery'].upper().startswith('SELECT DISTINCT'), 'Multiline SQL extraction failed'
    assert '"Customer_id"' in df.loc[4, 'SqlQuery'], 'Doubled-quote identifier extraction failed for Customer_id'
