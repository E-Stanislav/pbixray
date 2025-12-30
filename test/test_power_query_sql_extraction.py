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
                ,
                {'TableName': 'TestTable6', 'Expression': "let Source = Table.SelectRows(d_Em, each [Key] = 1) in Source"}
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
    # Non-SQL expression should not be treated as SQL
    assert df.loc[5, 'SqlQuery'] == '', 'Non-SQL expression must not produce SqlQuery'
    # SQL followed by trailing text after semicolon — extraction should stop at first semicolon
    handler2 = FakeHandler()
    # add a new row simulating trailing text after semicolon
    handler2.queries = []
    df2 = pd.DataFrame([
        {'TableName': 'TestTrailing', 'Expression': "let Source = Value.NativeQuery(Connector, \"SELECT a, b FROM t; RefreshModel()\") in Source"}
    ])
    # monkeypatch execute_query to return df2 for the m query
    def exec_q(sql):
        return df2 if 'FROM partition' in sql and 'p.Type = 4' in sql else pd.DataFrame()
    handler2.execute_query = exec_q
    handler2.close_connection = lambda: None
    mq2 = __import__('pbixray').meta.metadata_query.MetadataQuery(handler2)
    df_m2 = mq2.m_df
    assert df_m2.loc[0, 'SqlQuery'].strip().endswith(';'), 'Extraction should preserve semicolon at end of first statement'
    assert 'RefreshModel' not in df_m2.loc[0, 'SqlQuery'], 'Trailing text after semicolon should not be included in SqlQuery'
