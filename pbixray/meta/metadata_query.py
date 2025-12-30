
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
        # Patterns capture quoted SQL while allowing doubled quotes inside the quoted string
        # Note: keep single-quote branch as (?:[^']|'')* to properly handle doubled single quotes
        patterns = [
            rf"Value\.NativeQuery\([^,]*,\s*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')",
            rf"Query\s*=\s*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')",
            rf"Sql\.Database\([^)]*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')",
            rf"(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')"
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
                # Prefer extracting up to first semicolon (not inside quotes) — start is kept as-is
                truncated = self.__truncate_sql_to_first_statement(content)
                if ';' in truncated:
                    return truncated

                # If no semicolon, accept it only if it starts with a SQL starter
                if re.match(rf"^\s*{sql_starters}\b", truncated, re.IGNORECASE):
                    return truncated

                # Otherwise look for an embedded SQL statement (some expressions wrap additional text before SQL)
                sql_search = re.search(rf"([\s;]*({sql_starters})\b[\s\S]+)", content, re.IGNORECASE)
                if sql_search:
                    found_sql = sql_search.group(1).lstrip('\r\n\t ;').strip()
                    return self.__truncate_sql_to_first_statement(found_sql)

                # If we can't find an embedded SQL statement and no semicolon, try next pattern
                continue

        # As a fallback, try to handle concatenated SQL expressions passed to
        # Value.NativeQuery/Sql.Database (e.g., pieces joined with & and variables)
        concat_sql = self.__extract_sql_from_native_concat(expr)
        if concat_sql:
            truncated = self.__truncate_sql_to_first_statement(concat_sql)
            if ';' in truncated:
                return truncated
            if re.match(rf"^\s*{sql_starters}\b", truncated, re.IGNORECASE):
                return truncated

        return ''

    def __extract_sql_from_native_concat(self, expr: str) -> str:
        """Handle cases where the SQL passed to Value.NativeQuery or Sql.Database is built with concatenation.

        Attempts to find the function call, extract the second argument expression, then join all
        quoted literal pieces into a single SQL string (skipping variables), returning that
        for further processing. Returns empty string if nothing sensible is found.
        """
        import re
        i = 0
        lower = expr.lower()
        # look for common function names
        for fn in ('value.nativequery', 'sql.database'):
            idx = lower.find(fn)
            if idx == -1:
                continue
            # find opening parenthesis
            p = expr.find('(', idx)
            if p == -1:
                continue
            # parse until matching closing paren
            depth = 0
            j = p
            n = len(expr)
            in_squote = False
            in_dquote = False
            while j < n:
                ch = expr[j]
                if ch == '"' and not in_squote:
                    # handle doubled double quote
                    if in_dquote and j + 1 < n and expr[j + 1] == '"':
                        j += 2
                        continue
                    in_dquote = not in_dquote
                    j += 1
                    continue
                if ch == "'" and not in_dquote:
                    if in_squote and j + 1 < n and expr[j + 1] == "'":
                        j += 2
                        continue
                    in_squote = not in_squote
                    j += 1
                    continue
                if in_squote or in_dquote:
                    j += 1
                    continue
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            args_str = expr[p + 1:j] if j < n else expr[p + 1:]
            # split top-level args by commas (not within quotes or parentheses)
            args = []
            cur = []
            depth = 0
            in_squote = False
            in_dquote = False
            k = 0
            while k < len(args_str):
                ch = args_str[k]
                if ch == '"' and not in_squote:
                    if in_dquote and k + 1 < len(args_str) and args_str[k + 1] == '"':
                        cur.append('"')
                        k += 2
                        continue
                    in_dquote = not in_dquote
                    cur.append(ch)
                    k += 1
                    continue
                if ch == "'" and not in_dquote:
                    if in_squote and k + 1 < len(args_str) and args_str[k + 1] == "'":
                        cur.append("'")
                        k += 2
                        continue
                    in_squote = not in_squote
                    cur.append(ch)
                    k += 1
                    continue
                if in_squote or in_dquote:
                    cur.append(ch)
                    k += 1
                    continue
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                if ch == ',' and depth == 0:
                    args.append(''.join(cur).strip())
                    cur = []
                else:
                    cur.append(ch)
                k += 1
            if cur:
                args.append(''.join(cur).strip())
            if len(args) >= 2:
                second = args[1]
                # collect quoted literal pieces
                pieces = []
                for m in re.finditer(r'"((?:[^"]|"")*)"|\'((?:[^\']|\'\')*)\'', second):
                    piece = m.group(1) if m.group(1) is not None else m.group(2)
                    if piece is not None:
                        piece = piece.replace('""', '"').replace("''", "'")
                        pieces.append(piece)
                if not pieces:
                    # fallback: split by & (concatenation operator) and extract quoted segments
                    parts = [p.strip() for p in re.split(r'\s*&\s*', second)]
                    for seg in parts:
                        m2 = re.match(r'^(?:"([^"]*(?:""[^"]*)*)"|\'([^\']*(?:''[^\']*)*)\')$', seg)
                        if m2:
                            piece = m2.group(1) if m2.group(1) is not None else m2.group(2)
                            if piece is not None:
                                piece = piece.replace('""', '"').replace("''", "'")
                                pieces.append(piece)
                if pieces:
                    # join with spaces to maintain separators like parentheses and ORDER BY
                    return ' '.join(pieces)
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

    def __truncate_sql_to_first_statement(self, sql_text: str) -> str:
        """Return SQL up to and including the first semicolon that is not inside single or double quotes.

        If no such semicolon is found, return the whole trimmed SQL text.
        """
        i = 0
        n = len(sql_text)
        in_squote = False
        in_dquote = False
        while i < n:
            ch = sql_text[i]
            if ch == "'" and not in_dquote:
                # handle doubled single quote escapes inside single-quoted string
                if in_squote and i + 1 < n and sql_text[i + 1] == "'":
                    i += 2
                    continue
                in_squote = not in_squote
                i += 1
                continue
            if ch == '"' and not in_squote:
                # handle doubled double quote escapes inside double-quoted identifier/string
                if in_dquote and i + 1 < n and sql_text[i + 1] == '"':
                    i += 2
                    continue
                in_dquote = not in_dquote
                i += 1
                continue
            if ch == ';' and not in_squote and not in_dquote:
                # return up to and including semicolon
                return sql_text[:i + 1].strip()
            i += 1
        return sql_text.strip()

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