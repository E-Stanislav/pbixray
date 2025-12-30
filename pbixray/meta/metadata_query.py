
class MetadataQuery:
    def __init__(self, sqlite_handler):
        self.handler = sqlite_handler
        
        # Populate dataframes upon instantiation
        self.schema_df = self.__populate_schema()
        self.m_df = self.__populate_m()
        self.m_parameters_df = self.__populate_m_parameters()
        self.dax_tables_df = self.__populate_dax_tables()
        self.dax_measures_df = self.__populate_dax_measures()
        self.dax_columns_df = self.__populate_dax_columns()
        self.metadata_df = self.__populate_metadata()
        self.relationships_df = self.__populate_relationships()
        self.rls_df = self.__populate_rls()
        self.handler.close_connection()

    def __populate_schema(self):
        sql = """ 
        SELECT 
            t.Name AS TableName,
            c.ExplicitName AS ColumnName,
            sfd.FileName AS Dictionary, 
            sfh.FileName AS HIDX, 
            sfi.FileName AS IDF,
            cs.Statistics_DistinctStates as Cardinality,
            c.ExplicitDataType AS DataType,
            --ds.DataType,
            ds.BaseId,
            ds.Magnitude,
            ds.IsNullable,
            c.ModifiedTime,
            c.StructureModifiedTime
        FROM Column c 
        JOIN [Table] t ON c.TableId = t.ID
        JOIN ColumnStorage cs ON c.ColumnStorageID = cs.ID
        --HIDX
        JOIN AttributeHierarchy ah ON ah.ColumnID = c.ID
        JOIN AttributeHierarchyStorage ahs ON ah.AttributeHierarchyStorageID = ahs.ID
        LEFT JOIN StorageFile sfh ON sfh.ID = ahs.StorageFileID
        --Dictionary
        LEFT JOIN DictionaryStorage ds ON ds.ID = cs.DictionaryStorageID
        LEFT JOIN StorageFile sfd ON sfd.ID = ds.StorageFileID
        --IDF
        JOIN ColumnPartitionStorage cps ON cps.ColumnStorageID = cs.ID
        JOIN StorageFile sfi ON sfi.ID = cps.StorageFileID
        WHERE c.Type IN (1,2)
        ORDER BY t.Name, cs.StoragePosition
        """
        return self.handler.execute_query(sql)

    def __populate_m(self):
        sql = """ 
        SELECT 
            t.Name AS 'TableName', 
            p.QueryDefinition AS 'Expression'
        FROM partition p 
        JOIN [Table] t ON t.ID = p.TableID 
        WHERE p.Type = 4;
        """
        df = self.handler.execute_query(sql)
        # Add a cleaned SQL query column if a SQL statement is embedded in Expression
        if not df.empty and 'Expression' in df.columns:
            df = df.assign(SqlQuery=df['Expression'].apply(self.__extract_sql_from_expression))
        return df

    def __extract_sql_from_expression(self, expr):
        """Attempt to extract a plain SQL query from a Power Query (M) expression string.

        Looks for common patterns like Value.NativeQuery(..., "SELECT ..."), Query="SELECT ...",
        Sql.Database(..., "SELECT ...") and falls back to finding a quoted SELECT statement.
        """
        import re
        if not expr or not isinstance(expr, str):
            return ''

        # Allow SQL to start with a variety of common SQL keywords (SELECT, WITH, SET, etc.)
        sql_starters = r"(?:SELECT|WITH|SET|INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|EXEC|DECLARE)"
        # Patterns capture quoted SQL while allowing doubled quotes inside the quoted string
        patterns = [
            rf"Value\.NativeQuery\([^,]*,\s*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^\']|'' )*)')",
            rf"Query\s*=\s*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^\']|'' )*)')",
            rf"Sql\.Database\([^)]*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^\']|'' )*)')",
            rf"(?:\"((?:[^\"]|\"\")*)\"|'((?:[^\']|'' )*)')"
        ]

        for pat in patterns:
            m = re.search(pat, expr, re.IGNORECASE)
            if m:
                # prefer group 1 (double-quoted) else group 2 (single-quoted)
                content = m.group(1) if m.group(1) is not None else m.group(2)
                if not content:
                    continue
                # Unescape doubled quotes inside the quoted string
                content = content.replace('""', '"').replace("''", "'")
                # Try to find a SQL statement inside the content (allowing leading whitespace/newlines)
                sql_search = re.search(rf"([\s;]*({sql_starters})\b[\s\S]+)", content, re.IGNORECASE)
                if sql_search:
                    # group 1 contains leading whitespace + full SQL; strip leading whitespace
                    found_sql = sql_search.group(1).lstrip('\r\n\t ;').strip()
                    # If there are multiple statements or trailing non-SQL text, keep only up to the first semicolon
                    if ';' in found_sql:
                        first_stmt = found_sql.split(';', 1)[0].strip() + ';'
                        return first_stmt
                    return found_sql
                # If we can't find an embedded SQL statement, treat as non-SQL and return empty
                return ''
        return ''
    
    def __populate_m_parameters(self):
        sql = """ 
        SELECT 
            Name as ParameterName, 
            Description, 
            Expression, 
            ModifiedTime 
        FROM Expression;
        """
        return self.handler.execute_query(sql)  

    def __populate_dax_tables(self):
        sql = """ 
        SELECT 
            t.Name AS 'TableName', 
            p.QueryDefinition AS 'Expression'
        FROM partition p 
        JOIN [Table] t ON t.ID = p.TableID 
        WHERE p.Type = 2;
        """
        return self.handler.execute_query(sql)

    def __populate_dax_measures(self):
        sql = """ 
        SELECT 
            t.Name AS TableName,
            m.Name,
            m.Expression,
            m.DisplayFolder,
            m.Description
        FROM Measure m 
        JOIN [Table] t ON m.TableID = t.ID;
        """
        return self.handler.execute_query(sql)
    
    def __populate_dax_columns(self):
        sql = """ 
        SELECT 
            t.Name AS TableName,
            c.ExplicitName AS ColumnName,
            c.Expression
        FROM Column c 
        JOIN [Table] t ON c.TableID = t.ID
        WHERE c.Type = 2;
        """
        return self.handler.execute_query(sql)

    def __populate_metadata(self):
        sql = """
        SELECT Name,Value 
        FROM Annotation 
        WHERE ObjectType = 1
        """
        return self.handler.execute_query(sql)
    
    def __populate_relationships(self):
        sql = """
        SELECT 
            ft.Name AS FromTableName,
            fc.ExplicitName AS FromColumnName,
            tt.Name AS ToTableName,
            tc.ExplicitName AS ToColumnName,
            rel.IsActive,
            CASE 
                WHEN rel.FromCardinality = 2 THEN 'M'
                ELSE '1'
            END || ':' || 
            CASE 
                WHEN rel.ToCardinality = 2 THEN 'M'
                ELSE '1'
            END AS Cardinality,
            CASE 
                WHEN rel.CrossFilteringBehavior = 1 THEN 'Single'
                WHEN rel.CrossFilteringBehavior = 2 THEN 'Both'
                ELSE CAST(rel.CrossFilteringBehavior AS TEXT)
            END AS CrossFilteringBehavior,
            rid.RecordCount as FromKeyCount,
            rid2.RecordCount AS ToKeyCount,
            rel.RelyOnReferentialIntegrity
        FROM Relationship rel
            LEFT JOIN [Table] ft ON rel.FromTableID = ft.id
            LEFT JOIN [Column] fc ON rel.FromColumnID = fc.id
            LEFT JOIN [Table] tt ON rel.ToTableID = tt.id AND tt.systemflags = 0
            LEFT JOIN [Column] tc ON rel.ToColumnID = tc.id
            LEFT JOIN RelationshipStorage rs ON rs.id = rel.RelationshipStorageID
            LEFT JOIN RelationshipIndexStorage rid ON rs.RelationshipIndexStorageID = rid.id
            LEFT JOIN RelationshipStorage rs2 ON rs2.id = rel.RelationshipStorage2ID
            LEFT JOIN RelationshipIndexStorage rid2 ON rs2.RelationshipIndexStorageID = rid2.id
        """
        return self.handler.execute_query(sql)
    
    def __populate_rls(self):
        sql = """
        SELECT 
            t.Name as TableName,
            r.Name as RoleName,
            r.Description as RoleDescription,
            tp.FilterExpression,
            tp.State,
            tp.MetadataPermission
        FROM TablePermission tp
        JOIN [Table] t on t.ID = tp.TableID
        JOIN Role r on r.ID = tp.RoleID
        """
        return self.handler.execute_query(sql)