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
        if 'FROM partition' in sql and 'p.Type = 4' in sql and 'test_semicolon_in_quotes' in sql:
            # Used by a specific test that directly calls execute_query replacement
            return pd.DataFrame([
                {'TableName': 'Q1', 'Expression': "let Source = Value.NativeQuery(Connector, \"SELECT 'a; b' as col FROM t; RefreshModel()\") in Source"},
                {'TableName': 'Q2', 'Expression': "let Source = Value.NativeQuery(Connector, \"SELECT \"\"a; b\"\" as id FROM t; RefreshModel()\") in Source"}
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

    # Semicolon inside single-quoted string must not truncate SQL
    handler3 = FakeHandler()
    def exec_q2(sql):
        # return special DF for semicolon-in-quotes tests
        if 'FROM partition' in sql and 'p.Type = 4' in sql:
            return pd.DataFrame([
                {'TableName': 'Q1', 'Expression': "let Source = Value.NativeQuery(Connector, \"SELECT 'a; b' as col FROM t; RefreshModel()\") in Source"},
                {'TableName': 'Q2', 'Expression': "let Source = Value.NativeQuery(Connector, \"SELECT \"\"a; b\"\" as id FROM t; RefreshModel()\") in Source"}
            ])
        return pd.DataFrame()
    handler3.execute_query = exec_q2
    handler3.close_connection = lambda: None
    mq3 = __import__('pbixray').meta.metadata_query.MetadataQuery(handler3)
    df3 = mq3.m_df
    # first row contains single-quoted 'a; b' — extraction should not stop at that semicolon inside quotes,
    # but at the terminating semicolon after FROM t;
    assert df3.loc[0, 'SqlQuery'].endswith(';'), 'Should include terminating semicolon'
    assert "'a; b'" in df3.loc[0, 'SqlQuery'], 'Semicolon inside single quotes lost or caused truncation'
    # second row contains double-quoted identifier with semicolon; it should be preserved as well
    assert '"a; b"' in df3.loc[1, 'SqlQuery'], 'Semicolon inside double-quoted identifier lost or caused truncation'

    # Also test doubled single quotes inside SQL string: e.g., name = ''O''Reilly''
    handler4 = FakeHandler()
    def exec_q3(sql):
        if 'FROM partition' in sql and 'p.Type = 4' in sql:
            return pd.DataFrame([
                {'TableName': 'S1', 'Expression': "let Source = Value.NativeQuery(Connector, 'SELECT id FROM t WHERE name = ''O''Reilly'';') in Source"}
            ])
        return pd.DataFrame()
    handler4.execute_query = exec_q3
    handler4.close_connection = lambda: None
    mq4 = __import__('pbixray').meta.metadata_query.MetadataQuery(handler4)
    df4 = mq4.m_df
    assert "''O''Reilly''" in df4.loc[0, 'SqlQuery'] or "O'Reilly" in df4.loc[0, 'SqlQuery'], 'Doubled single quotes handling failed'

    # If content does not start with SQL but contains a semicolon, take up to the semicolon (start kept as-is)
    handler5 = FakeHandler()
    def exec_q4(sql):
        if 'FROM partition' in sql and 'p.Type = 4' in sql:
            return pd.DataFrame([
                {'TableName': 'T5', 'Expression': "let Source = Value.NativeQuery(Connector, \"prefix_without_keyword; REFRESH()\") in Source"}
            ])
        return pd.DataFrame()
    handler5.execute_query = exec_q4
    handler5.close_connection = lambda: None
    mq5 = __import__('pbixray').meta.metadata_query.MetadataQuery(handler5)
    df5 = mq5.m_df
    assert df5.loc[0, 'SqlQuery'].strip() == 'prefix_without_keyword;', 'Start-as-is semicolon-based extraction failed'

    # Concatenated SQL fragments (M expression joins literals with & and variables) should be reconstructed
    handler6 = FakeHandler()
    def exec_q5(sql):
        if 'FROM partition' in sql and 'p.Type = 4' in sql:
            return pd.DataFrame([
                {'TableName': 'Concat', 'Expression': "let Source = Value.NativeQuery(Connector, \"WHERE TRUE\n  AND ks.\"\"Код Региональной Группы\"\" IN ( \" & var_Reg & \" )\nORDER BY КС ASC\n;\") in Source"}
            ])
        return pd.DataFrame()
    handler6.execute_query = exec_q5
    handler6.close_connection = lambda: None
    mq6 = __import__('pbixray').meta.metadata_query.MetadataQuery(handler6)
    df6 = mq6.m_df
    assert 'IN (' in df6.loc[0, 'SqlQuery'], 'Concatenated IN clause not reconstructed'
    assert 'ORDER BY КС ASC' in df6.loc[0, 'SqlQuery'], 'ORDER BY lost in concatenated reconstruction'
    assert df6.loc[0, 'SqlQuery'].strip().endswith(';'), 'Concatenated SQL should end with ;'
