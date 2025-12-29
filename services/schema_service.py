"""Schema service for building schema context and table relationships"""
from typing import Dict, List, Optional
from services.bigquery_service import BigQueryService
import config
from utils.logger import ComponentLogger
from utils.tracing import trace_span


class SchemaService:
    """Service for managing database schema information"""
    
    def __init__(self, bigquery_service: Optional[BigQueryService] = None):
        """
        Initialize schema service
        
        Args:
            bigquery_service: Optional BigQueryService instance. Creates new if None.
        """
        self.logger = ComponentLogger("schema_service")
        self.bigquery_service = bigquery_service or BigQueryService()
        self._schema_cache: Dict[str, List[Dict]] = {}
        self._relationships = self._build_relationships()
        
        self.logger.info(
            "Schema service initialized",
            cached_tables=len(self._schema_cache),
            relationships_count=len(self._relationships),
            total_relations=sum(len(v) for v in self._relationships.values())
        )
    
    def _build_relationships(self) -> Dict[str, List[str]]:
        """
        Build table relationship mapping
        
        Returns:
            Dictionary mapping table names to related tables
        """
        relationships = {
            "orders": ["order_items", "users"],
            "order_items": ["orders", "products"],
            "products": ["order_items"],
            "users": ["orders"]
        }
        
        # Note: Logger may not be initialized yet during __init__, so we log in __init__ instead
        return relationships
    
    def get_table_schema(self, table_name: str, use_cache: bool = True) -> List[Dict]:
        """
        Get schema for a table with optional caching
        
        Args:
            table_name: Name of the table
            use_cache: Whether to use cached schema
            
        Returns:
            List of field dictionaries
        """
        with trace_span("get_table_schema", component="schema_service", table_name=table_name) as span:
            if use_cache and table_name in self._schema_cache:
                self.logger.debug(
                    "Schema cache hit",
                    table_name=table_name,
                    cache_size=len(self._schema_cache)
                )
                span.set_tag("cache_hit", True)
                return self._schema_cache[table_name]
            
            self.logger.debug(
                "Schema cache miss, fetching from BigQuery",
                table_name=table_name,
                cache_size=len(self._schema_cache)
            )
            span.set_tag("cache_hit", False)
            
            schema = self.bigquery_service.get_table_schema(table_name)
            
            if use_cache:
                self._schema_cache[table_name] = schema
                self.logger.debug(
                    "Schema cached",
                    table_name=table_name,
                    field_count=len(schema),
                    cache_size=len(self._schema_cache)
                )
            
            span.set_tag("field_count", len(schema))
            return schema
    
    def get_all_schemas(self) -> Dict[str, List[Dict]]:
        """
        Get schemas for all required tables
        
        Returns:
            Dictionary mapping table names to their schemas
        """
        with trace_span("get_all_schemas", component="schema_service") as span:
            self.logger.debug(
                "Fetching all schemas",
                table_count=len(config.REQUIRED_TABLES),
                cache_size=len(self._schema_cache)
            )
            
            schemas = {}
            for table_name in config.REQUIRED_TABLES:
                schemas[table_name] = self.get_table_schema(table_name)
            
            self.logger.info(
                "All schemas fetched",
                table_count=len(schemas),
                total_fields=sum(len(s) for s in schemas.values())
            )
            
            span.set_tag("table_count", len(schemas))
            return schemas
    
    def build_column_location_map(self) -> Dict[str, List[str]]:
        """
        Build map of column names to tables

        Returns:
            Dictionary mapping column names to list of tables containing them
        """
        with trace_span("build_column_location_map", component="schema_service") as span:
            self.logger.debug("Building column location map")
            column_map = {}
            schemas = self.get_all_schemas()
            
            for table_name, schema in schemas.items():
                for field in schema:
                    col_name = field['name']
                    if col_name not in column_map:
                        column_map[col_name] = []
                    column_map[col_name].append(table_name)
            
            self.logger.debug(
                "Column location map built",
                total_columns=len(column_map),
                ambiguous_columns=sum(1 for tables in column_map.values() if len(tables) > 1)
            )
            span.set_tag("total_columns", len(column_map))
            return column_map

    def build_schema_context(self, include_examples: bool = True) -> str:
        """
        Build a formatted schema context string for prompts

        Args:
            include_examples: Whether to include example queries

        Returns:
            Formatted schema context string
        """
        with trace_span("build_schema_context", component="schema_service", include_examples=include_examples) as span:
            self.logger.debug(
                "Building schema context",
                include_examples=include_examples
            )

            schemas = self.get_all_schemas()
            context_parts = []

            context_parts.append("DATABASE SCHEMA:")
            context_parts.append("=" * 60)

            for table_name in config.REQUIRED_TABLES:
                schema = schemas[table_name]
                context_parts.append(f"\nTable: {table_name}")
                context_parts.append("-" * 40)

                # Add relationships
                if table_name in self._relationships:
                    related = ", ".join(self._relationships[table_name])
                    context_parts.append(f"Related tables: {related}")

                # Add columns
                context_parts.append("Columns:")
                for field in schema:
                    field_desc = f"  - {field['name']}: {field['type']}"
                    if field['mode'] == 'REPEATED':
                        field_desc += " (ARRAY)"
                    if field['description']:
                        field_desc += f" -- {field['description']}"
                    context_parts.append(field_desc)

            # Add column location mapping
            context_parts.append("\n" + "=" * 60)
            context_parts.append("COLUMN LOCATIONS (Important for JOINs):")
            context_parts.append("-" * 60)
            column_map = self.build_column_location_map()
            for col, tables in sorted(column_map.items()):
                if len(tables) == 1:
                    context_parts.append(f"  - {col}: {tables[0]}")
                else:
                    context_parts.append(f"  - {col}: {', '.join(tables)} (appears in multiple tables)")
            
            # Highlight important columns
            important_columns = {
                "country": "users",
                "state": "users",
                "city": "users",
                "user_id": ["orders", "order_items", "users"],
                "order_id": ["orders", "order_items"],
                "product_id": ["products", "order_items"]
            }
            context_parts.append("\nKEY COLUMN LOCATIONS:")
            context_parts.append("-" * 60)
            for col, table_info in important_columns.items():
                if isinstance(table_info, list):
                    context_parts.append(f"  - {col}: {', '.join(table_info)}")
                else:
                    context_parts.append(f"  - {col}: {table_info}")
            
            if include_examples:
                context_parts.append("\n" + "=" * 60)
                context_parts.append("EXAMPLE QUERIES:")
                context_parts.append("-" * 40)
                context_parts.append("1. Count orders: SELECT COUNT(*) FROM `bigquery-public-data.thelook_ecommerce.orders`")
                context_parts.append("2. Top products: SELECT p.name, SUM(oi.sale_price) as revenue")
                context_parts.append("   FROM `bigquery-public-data.thelook_ecommerce.products` p")
                context_parts.append("   JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON p.id = oi.product_id")
                context_parts.append("   GROUP BY p.name ORDER BY revenue DESC LIMIT 10")
            
            context_str = "\n".join(context_parts)
            
            self.logger.info(
                "Schema context built",
                context_length=len(context_str),
                include_examples=include_examples,
                table_count=len(config.REQUIRED_TABLES)
            )
            
            span.set_tag("context_length", len(context_str))
            span.set_tag("table_count", len(config.REQUIRED_TABLES))
            
            return context_str
    
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get comprehensive information about a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        with trace_span("get_table_info", component="schema_service", table_name=table_name) as span:
            self.logger.debug(
                "Getting table info",
                table_name=table_name
            )
            
            schema = self.get_table_schema(table_name)
            relationships = self._relationships.get(table_name, [])
            
            info = {
                "name": table_name,
                "columns": schema,
                "column_count": len(schema),
                "related_tables": relationships,
                "full_name": f"{config.DATASET_ID}.{table_name}"
            }
            
            self.logger.debug(
                "Table info retrieved",
                table_name=table_name,
                column_count=len(schema),
                related_tables_count=len(relationships)
            )
            
            span.set_tag("column_count", len(schema))
            span.set_tag("related_tables_count", len(relationships))
            
            return info
    
    def clear_cache(self):
        """Clear the schema cache"""
        cache_size_before = len(self._schema_cache)
        self._schema_cache.clear()
        
        self.logger.info(
            "Schema cache cleared",
            entries_cleared=cache_size_before
        )

