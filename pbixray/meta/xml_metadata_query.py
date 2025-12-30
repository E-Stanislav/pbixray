import pandas as pd
import re
from datetime import datetime
from ..utils import get_data_slice
from ..xldm import (
    CubXmlLoad,
    DimensionXmlLoad,
    PartitionXmlLoad,
    MeasureGroupXmlLoad,
    MdxScriptXmlLoad,
    DataSourceXmlLoad,
    DataSourceViewXmlLoad
)
from ..xldm.xmobject import XMObjectDocument

class XmlMetadataQuery:
    """Handles metadata extraction from XML files in XLSX Power Pivot models."""
    
    def __init__(self, data_model):
        self.data_model = data_model
        self._cube = None
        self._dimensions = {}
        self._partitions = {}
        self._measure_groups = {}
        self._mdx_script = None
        self._data_sources = {}
        self._data_source_view = None
        self._tbl_objects = {}
        
        self._parse_cube()
        self._extract_dimension_metadata()
        self._extract_partition_metadata()
        self._extract_measure_group_metadata()
        self._extract_mdx_script()
        self._extract_data_sources()
        self._extract_data_source_view()
        self._extract_tbl_metadata()
        # Build Power Query (M) table information
        self._build_m()
        
        self._build_schema()
        self._build_dax_tables()
        self._build_dax_measures()
        self._build_dax_columns()
        self._build_relationships()
        
        self.m_df = pd.DataFrame()
        self.m_parameters_df = pd.DataFrame()
        self.metadata_df = pd.DataFrame()
        self.rls_df = pd.DataFrame()
    
    def _parse_cube(self):
        cube_pattern = re.compile(r'Model\.\d+\.cub\.xml')
        for file_entry in self.data_model.file_log:
            if cube_pattern.match(file_entry['FileName']):
                cube_content = get_data_slice(self.data_model, file_entry['FileName'])
                cube_xml = CubXmlLoad.from_xml_string(cube_content.decode('utf-8'))
                self._cube = cube_xml.Cube
                return
        raise RuntimeError("Model .cub.xml file not found in the data model")
    
    def _extract_dimension_metadata(self):
        dim_pattern = re.compile(r'(.+)\.(\d+)\.dim\.xml')
        for file_entry in self.data_model.file_log:
            match = dim_pattern.match(file_entry['FileName'])
            if match:
                dimension_id = match.group(1)
                try:
                    dim_content = get_data_slice(self.data_model, file_entry['FileName'])
                    dim_xml = DimensionXmlLoad.from_xml_string(dim_content.decode('utf-8'))
                    self._dimensions[dimension_id] = dim_xml.Dimension
                except Exception as e:
                    print(f"Error parsing dimension file {file_entry['FileName']}: {e}")
    
    def _extract_partition_metadata(self):
        prt_pattern = re.compile(r'(.+)\.(\d+)\.prt\.xml')
        for file_entry in self.data_model.file_log:
            match = prt_pattern.match(file_entry['FileName'])
            if match:
                dimension_id = match.group(1)
                try:
                    prt_content = get_data_slice(self.data_model, file_entry['FileName'])
                    prt_xml = PartitionXmlLoad.from_xml_string(prt_content.decode('utf-8'))
                    self._partitions[dimension_id] = prt_xml.Partition
                except Exception as e:
                    print(f"Error parsing partition file {file_entry['FileName']}: {e}")
    
    def _extract_measure_group_metadata(self):
        det_pattern = re.compile(r'(.+)\.(\d+)\.det\.xml')
        for file_entry in self.data_model.file_log:
            match = det_pattern.match(file_entry['FileName'])
            if match:
                dimension_id = match.group(1)
                try:
                    det_content = get_data_slice(self.data_model, file_entry['FileName'])
                    det_xml = MeasureGroupXmlLoad.from_xml_string(det_content.decode('utf-8'))
                    self._measure_groups[dimension_id] = det_xml.MeasureGroup
                except Exception as e:
                    print(f"Error parsing measure group file {file_entry['FileName']}: {e}")
    
    def _extract_mdx_script(self):
        scr_pattern = re.compile(r'MdxScript\.\d+\.scr\.xml')
        for file_entry in self.data_model.file_log:
            if scr_pattern.match(file_entry['FileName']):
                try:
                    scr_content = get_data_slice(self.data_model, file_entry['FileName'])
                    scr_xml = MdxScriptXmlLoad.from_xml_string(scr_content.decode('utf-8'))
                    self._mdx_script = scr_xml.MdxScript
                    return
                except Exception as e:
                    print(f"Error parsing MDX script file {file_entry['FileName']}: {e}")
    
    def _extract_data_sources(self):
        ds_pattern = re.compile(r'([a-f0-9\-]+)\.\d+\.ds\.xml')
        for file_entry in self.data_model.file_log:
            match = ds_pattern.match(file_entry['FileName'])
            if match:
                ds_id = match.group(1)
                try:
                    ds_content = get_data_slice(self.data_model, file_entry['FileName'])
                    ds_xml = DataSourceXmlLoad.from_xml_string(ds_content.decode('utf-8'))
                    self._data_sources[ds_id] = ds_xml.DataSource
                except Exception as e:
                    print(f"Error parsing data source file {file_entry['FileName']}: {e}")
    
    def _extract_data_source_view(self):
        dsv_pattern = re.compile(r'.+\.\d+\.dsv\.xml')
        for file_entry in self.data_model.file_log:
            if dsv_pattern.match(file_entry['FileName']):
                try:
                    dsv_content = get_data_slice(self.data_model, file_entry['FileName'])
                    dsv_xml = DataSourceViewXmlLoad.from_xml_string(dsv_content.decode('utf-8'))
                    self._data_source_view = dsv_xml.DataSourceView
                    return
                except Exception as e:
                    print(f"Error parsing data source view file {file_entry['FileName']}: {e}")
    
    def _extract_tbl_metadata(self):
        tbl_pattern = re.compile(r'^([^H$R$][^$]*?)\.(\d+)\.tbl\.xml$')
        for file_entry in self.data_model.file_log:
            match = tbl_pattern.match(file_entry['FileName'])
            if match:
                dimension_id = match.group(1)
                try:
                    tbl_content = get_data_slice(self.data_model, file_entry['FileName'])
                    tbl_doc = XMObjectDocument.from_xml_string(tbl_content.decode('utf-8'))
                    self._tbl_objects[dimension_id] = tbl_doc.root_object
                except Exception as e:
                    print(f"Error parsing table file {file_entry['FileName']}: {e}")
    
    def _build_schema(self):
        schema_data = []
        if self._cube and self._cube.Dimensions:
            for cube_dim in self._cube.Dimensions:
                dimension_id = cube_dim.DimensionID
                table_name = cube_dim.Name
                is_visible = cube_dim.Visible
                dimension = self._dimensions.get(dimension_id)
                tbl_obj = self._tbl_objects.get(dimension_id)
                
                if dimension and dimension.Attributes:
                    for attr in dimension.Attributes:
                        column_name = attr.Name
                        stats = self._get_column_stats_from_tbl(tbl_obj, column_name)
                        file_info = self._find_column_files(dimension_id, column_name)
                        data_type = self._map_attribute_type_to_pandas(attr)
                        
                        column_data = {
                            'TableName': table_name,
                            'ColumnName': column_name,
                            'Dictionary': file_info.get('dictionary', ''),
                            'HIDX': file_info.get('hidx', ''),
                            'IDF': file_info.get('idf', ''),
                            'Cardinality': stats.get('cardinality', 0),
                            'DataType': data_type,
                            'BaseId': stats.get('base_id', 0),
                            'Magnitude': stats.get('magnitude', 0),
                            'IsNullable': stats.get('is_nullable', True),
                            'ModifiedTime': dimension.LastProcessed,
                            'StructureModifiedTime': dimension.LastSchemaUpdate,
                            'DimensionID': dimension_id,
                            'Visible': is_visible,
                            'RLERuns': stats.get('rle_runs', 0),
                            'MinDataID': stats.get('min_data_id', 0),
                            'MaxDataID': stats.get('max_data_id', 0),
                            'CompressionType': stats.get('compression_type', 0),
                            'HasNulls': stats.get('has_nulls', False)
                        }
                        schema_data.append(column_data)
        self.schema_df = pd.DataFrame(schema_data)
    
    def _get_column_stats_from_tbl(self, tbl_obj, column_name):
        stats = {
            'cardinality': 0,
            'base_id': 0,
            'magnitude': 0,
            'is_nullable': True,
            'rle_runs': 0,
            'min_data_id': 0,
            'max_data_id': 0,
            'compression_type': 0,
            'has_nulls': False
        }
        
        if not tbl_obj or not tbl_obj.collections:
            return stats
        
        for collection in tbl_obj.collections:
            if collection.Name == "Columns":
                for xm_obj in collection.XMObjects:
                    if xm_obj.name == column_name and xm_obj.class_name == "XMRawColumn":
                        # Get cardinality from Hierarchy member (DistinctDataIDs)
                        for member in xm_obj.members:
                            if member.Name == "IntrinsicHierarchy" and member.XMObject:
                                hierarchy_obj = member.XMObject
                                if hierarchy_obj.properties and hasattr(hierarchy_obj.properties, 'DistinctDataIDs'):
                                    stats['cardinality'] = hierarchy_obj.properties.DistinctDataIDs
                            elif member.Name == "ColumnStats" and member.XMObject:
                                stats_obj = member.XMObject
                                if stats_obj.properties:
                                    if hasattr(stats_obj.properties, 'MinDataID'):
                                        stats['min_data_id'] = stats_obj.properties.MinDataID
                                    if hasattr(stats_obj.properties, 'MaxDataID'):
                                        stats['max_data_id'] = stats_obj.properties.MaxDataID
                                    if hasattr(stats_obj.properties, 'HasNulls'):
                                        stats['has_nulls'] = stats_obj.properties.HasNulls
                                    if hasattr(stats_obj.properties, 'RLERuns'):
                                        stats['rle_runs'] = stats_obj.properties.RLERuns
                                    if hasattr(stats_obj.properties, 'CompressionType'):
                                        stats['compression_type'] = stats_obj.properties.CompressionType
                        break
        return stats
    
    def _map_attribute_type_to_pandas(self, attr):
        if attr.KeyColumns:
            for key_col in attr.KeyColumns:
                if key_col.DataType:
                    return self._map_ssas_type_to_pandas(key_col.DataType)
        return 'object'
    
    def _map_ssas_type_to_pandas(self, ssas_type):
        type_map = {
            'WChar': 'string',
            'Integer': 'int64',
            'BigInt': 'int64',
            'Double': 'float64',
            'Date': 'datetime64[ns]',
            'Boolean': 'bool',
            'Currency': 'float64',
            'Variant': 'object',
            'Empty': 'object'  # Empty/null data type
        }
        return type_map.get(ssas_type, 'object')
    
    def _find_column_files(self, dimension_id, column_name):
        files = {'dictionary': '', 'hidx': '', 'idf': ''}
        for file_entry in self.data_model.file_log:
            file_name = file_entry['FileName']
            if (dimension_id in file_name or f"H${dimension_id}" in file_name) and column_name in file_name:
                if '.dictionary' in file_name and not '.ID_TO_POS.' in file_name and not '.POS_TO_ID.' in file_name:
                    if f".{column_name}.0.idf.dictionary" in file_name or f".{column_name}.dictionary" in file_name:
                        files['dictionary'] = file_name
                elif '.hidx' in file_name:
                    if f"${column_name}.hidx" in file_name or f"${column_name}.POS_TO_ID.0.idf.hidx" in file_name:
                        files['hidx'] = file_name
                elif '.idf' in file_name and '.ID_TO_POS.' not in file_name and '.POS_TO_ID.' not in file_name and '.hidx' not in file_name:
                    if f".{column_name}.0.idf" in file_name:
                        files['idf'] = file_name
        return files
    
    def _build_dax_tables(self):
        dax_tables_data = []
        for dimension_id, partition in self._partitions.items():
            if partition and partition.Source:
                dimension = self._dimensions.get(dimension_id)
                table_name = dimension.Name if dimension else dimension_id
                query_definition = self._extract_query_from_source(partition.Source)
                if query_definition:
                    dax_tables_data.append({
                        'TableName': table_name,
                        'Expression': query_definition
                    })
        self.dax_tables_df = pd.DataFrame(dax_tables_data)
    
    def _extract_query_from_source(self, source):
        if not source:
            return None
        if hasattr(source, 'QueryDefinition'):
            return source.QueryDefinition
        if hasattr(source, 'Source') and source.Source:
            if hasattr(source.Source, 'ColumnID'):
                return f"[{source.Source.ColumnID}]"
        return None

    def _build_m(self):
        """Build the Power Query (M) dataframe similar to the SQL-backed MetadataQuery."""
        m_data = []
        for dimension_id, partition in self._partitions.items():
            if partition and partition.Source:
                dimension = self._dimensions.get(dimension_id)
                table_name = dimension.Name if dimension else dimension_id
                query_definition = self._extract_query_from_source(partition.Source)
                if query_definition:
                    m_data.append({
                        'TableName': table_name,
                        'Expression': query_definition
                    })
        self.m_df = pd.DataFrame(m_data)
        if not self.m_df.empty and 'Expression' in self.m_df.columns:
            self.m_df = self.m_df.assign(SqlQuery=self.m_df['Expression'].apply(self._extract_sql_from_expression))

    def _extract_sql_from_expression(self, expr):
        """Try to pull a SQL statement from an M Expression string (best-effort)."""
        import re
        if not expr or not isinstance(expr, str):
            return ''

        sql_starters = r"(?:SELECT|WITH|SET|INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|EXEC|DECLARE)"
        # Patterns capture quoted SQL while allowing doubled quotes inside the quoted string
        patterns = [
            rf"Value\.NativeQuery\([^,]*,\s*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')",
            rf"Query\s*=\s*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')",
            rf"Sql\.Database\([^)]*(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')",
            rf"(?:\"((?:[^\"]|\"\")*)\"|'((?:[^']|'')*)')"
        ]

        for pat in patterns:
            m = re.search(pat, expr, re.IGNORECASE)
            if m:
                content = m.group(1) if m.group(1) is not None else m.group(2)
                if not content:
                    continue
                content = content.replace('""', '"').replace("''", "'")
                # Prefer truncating at semicolon (keep start as-is)
                truncated = self._truncate_sql_to_first_statement(content)
                if ';' in truncated:
                    return truncated

                # If it starts with a SQL starter, accept it
                if re.match(rf"^\s*{sql_starters}\b", truncated, re.IGNORECASE):
                    return truncated

                sql_search = re.search(rf"([\s;]*({sql_starters})\b[\s\S]+)", content, re.IGNORECASE)
                if sql_search:
                    found_sql = sql_search.group(1).lstrip('\r\n\t ;').strip()
                    return self._truncate_sql_to_first_statement(found_sql)

                return ''
        return ''

    def _extract_sql_from_native_concat(self, expr: str) -> str:
        """Same logic as MetadataQuery.__extract_sql_from_native_concat but for XmlMetadataQuery.
        """
        import re
        i = 0
        lower = expr.lower()
        for fn in ('value.nativequery', 'sql.database'):
            idx = lower.find(fn)
            if idx == -1:
                continue
            p = expr.find('(', idx)
            if p == -1:
                continue
            depth = 0
            j = p
            n = len(expr)
            in_squote = False
            in_dquote = False
            while j < n:
                ch = expr[j]
                if ch == '"' and not in_squote:
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
            # split top-level args by commas
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
                pieces = []
                for m in re.finditer(r'"((?:[^"]|"")*)"|\'((?:[^\']|\'\')*)\'', second):
                    piece = m.group(1) if m.group(1) is not None else m.group(2)
                    if piece is not None:
                        piece = piece.replace('""', '"').replace("''", "'")
                        pieces.append(piece)
                if not pieces:
                    parts = [p.strip() for p in re.split(r'\s*&\s*', second)]
                    for seg in parts:
                        m2 = re.match(r'^(?:"([^"]*(?:""[^"]*)*)"|\'([^\']*(?:''[^\']*)*)\')$', seg)
                        if m2:
                            piece = m2.group(1) if m2.group(1) is not None else m2.group(2)
                            if piece is not None:
                                piece = piece.replace('""', '"').replace("''", "'")
                                pieces.append(piece)
                if pieces:
                    return ' '.join(pieces)
        return ''
        # fallback: try to handle concatenation-based SQL passed into native functions
        concat_sql = self._extract_sql_from_native_concat(expr)
        if concat_sql:
            truncated = self._truncate_sql_to_first_statement(concat_sql)
            if ';' in truncated:
                return truncated
            if re.match(rf"^\s*{sql_starters}\b", truncated, re.IGNORECASE):
                return truncated
        return ''

    def _truncate_sql_to_first_statement(self, sql_text: str) -> str:
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
                if in_squote and i + 1 < n and sql_text[i + 1] == "'":
                    i += 2
                    continue
                in_squote = not in_squote
                i += 1
                continue
            if ch == '"' and not in_squote:
                if in_dquote and i + 1 < n and sql_text[i + 1] == '"':
                    i += 2
                    continue
                in_dquote = not in_dquote
                i += 1
                continue
            if ch == ';' and not in_squote and not in_dquote:
                return sql_text[:i + 1].strip()
            i += 1
        return sql_text.strip()
    
    def _build_dax_measures(self):
        measures_data = []
        for dimension_id, measure_group in self._measure_groups.items():
            if measure_group and measure_group.Measures:
                dimension = self._dimensions.get(dimension_id)
                table_name = dimension.Name if dimension else dimension_id
                for measure in measure_group.Measures:
                    measures_data.append({
                        'TableName': table_name,
                        'Name': measure.Name,
                        'Expression': measure.MeasureExpression if measure.MeasureExpression else '',
                        'DisplayFolder': measure.DisplayFolder if measure.DisplayFolder else '',
                        'Description': measure.Description if measure.Description else ''
                    })
        self.dax_measures_df = pd.DataFrame(measures_data)
    
    def _build_dax_columns(self):
        self.dax_columns_df = pd.DataFrame()
    
    def _build_relationships(self):
        relationships_data = []
        
        # Iterate through dimensions and their relationships
        for dimension_id, dimension in self._dimensions.items():
            if not dimension or not dimension.Relationships:
                continue
            
            # Get the table name for the current dimension (from relationship end)
            from_table_name = dimension.Name if dimension.Name else dimension_id
            
            for relationship in dimension.Relationships:
                if not relationship.FromRelationshipEnd or not relationship.ToRelationshipEnd:
                    continue
                
                # Get From (foreign key) information
                from_dimension_id = relationship.FromRelationshipEnd.DimensionID
                from_attributes = relationship.FromRelationshipEnd.Attributes
                from_multiplicity = relationship.FromRelationshipEnd.Multiplicity
                
                # Get To (primary key) information
                to_dimension_id = relationship.ToRelationshipEnd.DimensionID
                to_attributes = relationship.ToRelationshipEnd.Attributes
                to_multiplicity = relationship.ToRelationshipEnd.Multiplicity
                
                # Map dimension IDs to table names
                from_table = self._dimensions.get(from_dimension_id).Name if from_dimension_id in self._dimensions else from_dimension_id
                to_table = self._dimensions.get(to_dimension_id).Name if to_dimension_id in self._dimensions else to_dimension_id
                
                # Get column names from attributes
                from_column = from_attributes[0].AttributeID if from_attributes else ''
                to_column = to_attributes[0].AttributeID if to_attributes else ''
                
                # Map attribute IDs to actual column names
                if from_dimension_id in self._dimensions:
                    from_dim = self._dimensions[from_dimension_id]
                    for attr in from_dim.Attributes:
                        if attr.ID == from_column:
                            from_column = attr.Name
                            break
                
                if to_dimension_id in self._dimensions:
                    to_dim = self._dimensions[to_dimension_id]
                    for attr in to_dim.Attributes:
                        if attr.ID == to_column:
                            to_column = attr.Name
                            break
                
                # Determine cardinality from multiplicities
                cardinality = self._map_multiplicity_to_cardinality(from_multiplicity, to_multiplicity)
                
                relationships_data.append({
                    'FromTableName': from_table,
                    'FromColumnName': from_column,
                    'ToTableName': to_table,
                    'ToColumnName': to_column,
                    'IsActive': relationship.Visible,
                    'Cardinality': cardinality,
                    'CrossFilteringBehavior': 'Single',
                    'FromKeyCount': 0,
                    'ToKeyCount': 0,
                    'RelyOnReferentialIntegrity': False
                })
        
        self.relationships_df = pd.DataFrame(relationships_data)
    
    def _map_multiplicity_to_cardinality(self, from_multiplicity, to_multiplicity):
        """Map relationship multiplicities to Power BI cardinality notation."""
        if from_multiplicity == 'Many' and to_multiplicity == 'One':
            return 'M:1'
        elif from_multiplicity == 'One' and to_multiplicity == 'Many':
            return '1:M'
        elif from_multiplicity == 'One' and to_multiplicity == 'One':
            return '1:1'
        elif from_multiplicity == 'Many' and to_multiplicity == 'Many':
            return 'M:M'
        else:
            return 'M:1'  # Default fallback