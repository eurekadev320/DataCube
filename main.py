import sqlite3
import configparser
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from fastapi.openapi.utils import get_openapi


class Node(BaseModel):
    id: int
    name: str
    parent_id: int = None


config = configparser.ConfigParser()
config.read('config.ini')

db_file = config['DATABASE']['DBFile']
fact_table = config['SCHEMA']['FactTable']
dim_tables = config['SCHEMA']['DimTables'].split(',')
aggregateColumn = config['DATA']['FactColumn']

app = FastAPI()


def init_db():
    db = sqlite3.connect(db_file)
    db.row_factory = sqlite3.Row
    return db


@app.on_event("startup")
def startup():
    init_db()


@app.get("/openapi.json")
async def get_open_api_endpoint():
    return get_openapi(title="OpenOLAP", version="1.0.0", routes=app.routes)


@app.get("/mapData")
def build_mappings():
    db = init_db()
    create_mapping_tables(db)
    return {"message": "Mappings generated"}


def create_mapping_tables(db):

    for dim in dim_tables:
        dim = dim.lower()

        id_col = f"{dim}_id"

        rows = db.execute(f"SELECT * FROM {dim}").fetchall()
        rows = [dict(row) for row in rows]

        nodes = {
            row[id_col]: Node(
                id=row[id_col],
                name=row['name'],
                parent_id=row['parent_id']
            ) for row in rows
        }

        mapping = generate_mapping(nodes, root_id=1)

        if db.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{dim}_map'").fetchone():
            db.execute(f"DROP TABLE {dim}_map")

        db.execute(f"CREATE TABLE {dim}_map ({id_col} INTEGER, parent_{id_col} INTEGER)")

        for child, parents in mapping.items():
            for parent in parents:
                db.execute(f"INSERT INTO {dim}_map VALUES (?, ?)", (child, parent))

    db.commit()
    db.close()
    return {"message": "success"}


def generate_mapping(nodes, root_id):
    mapping = {}

    for node_id, node in nodes.items():
        mapping.setdefault(node_id, []).append(node_id)

        parent = node
        while parent is not None:
            if parent.id != node_id:
                mapping[node_id].append(parent.id)

            if parent.parent_id is not None:
                parent = nodes[parent.parent_id]
            else:
                parent = None

    return mapping


@app.post("/queryCell")
async def get_data(request: Request):

    data = await request.json()

    filters = validate_filters(data)
    if 'error' in filters:
        return filters

    dimensions = parse_filters(filters)

    query = build_query(fact_table, dimensions)

    result = execute_query(query)

    return {"result": result}


def validate_filters(filters):
    if not isinstance(filters, dict):
        return {"error": "Filters must be a dictionary"}

    for dimension, member in filters.items():
        if not isinstance(dimension, str) or not isinstance(member, str):
            return {"error": "Filter dimensions and members must be strings"}

    return filters


def parse_filters(filters):
    dimensions = []

    for dimension, member in filters.items():
        dimensions.append({
            "dimension": dimension,
            "member": member
        })

    return dimensions


def build_query(fact_table, dimensions):
    query = f"SELECT SUM({aggregateColumn}) FROM {fact_table} s"

    for dim in dim_tables:
        query += f"""
      LEFT JOIN {dim}_map dm ON s.{dim}_id = dm.{dim}_id
    """

    filters = []
    for dim in dimensions:
        filters.append(f"""
      dm.parent_{dim['dimension']}_id IN (
        SELECT {dim['dimension']}_id FROM {dim['dimension']}
        WHERE name = ? OR alias = ?  
      )
    """)

    if filters:
        query += " WHERE " + " AND ".join(filters)

    return query, [dim['member'] for dim in dimensions for _ in (0, 1)]  # Flatten the list


def execute_query(query):
    query_string, params = query
    with sqlite3.connect(db_file) as db:
        result = db.execute(query_string, params).fetchone()[0]

    return result


@app.post("/pivot")
async def pivot(request: Request):
    data = await request.json()

    if not data or not all(f in data for f in ["rows", "columns"]):
        return {"error": "Invalid request"}

    rows = data["rows"]
    columns = data["columns"]
    filters = data["filters"]

    dimensions = {
        "rows": rows,
        "columns": columns,
        "filters": filters,
        "fact_table": fact_table,
        "dim_tables": dim_tables
    }

    sql = generate_pivot(dimensions)

    with sqlite3.connect(db_file) as db:
        cursor = db.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in cursor.fetchall()]

    response_payload = {
        "filters": dimensions["filters"],
        "rows": dimensions["rows"],
        "columns":dimensions["columns"],
        "data": result
    }

    return response_payload


def generate_pivot(params):
    fact_table = params["fact_table"]
    dim_tables = params["dim_tables"]
    rows = params["rows"]
    columns = params["columns"]
    filters = params["filters"]

    sql_query = "SELECT "

    # Alias mapping for dimension tables
    alias_map = {dim: f"d{idx}" for idx, dim in enumerate(dim_tables)}
    default_aggregate = "SUM"

    # Construct the column for row dimensions
    row_dimensions = []
    for row in rows:
        dimension_alias = alias_map[row['dimension']]
        row_dimensions.append(f"{dimension_alias}.name AS {row['dimension']}_name")

        sql_query = "SELECT " + ", ".join(row_dimensions) + ", "

    # Aggregate columns
    column_expressions = []
    for column in columns:
        dim_alias = alias_map[column['dimension']]
        aggregate = column.get('aggregate', default_aggregate)
        # Create a subquery to handle rollups or leaves
        rollup_subquery = f"(SELECT {column['dimension']}_id FROM {column['dimension']} WHERE name = '{column['member']}' OR {column['dimension']}_id IN (SELECT {column['dimension']}_id FROM {column['dimension']}_map WHERE parent_{column['dimension']}_id = (SELECT {column['dimension']}_id FROM {column['dimension']} WHERE name = '{column['member']}')))"

        expression = f"{aggregate}(CASE WHEN {dim_alias}.{column['dimension']}_id IN {rollup_subquery} THEN sales ELSE 0 END) AS {column.get('alias', column['member'])}"

        column_expressions.append(expression)

    sql_query += ", ".join(column_expressions)
    sql_query += f" FROM {fact_table} f "

    # Join operations for dimension tables
    for index, dimension in enumerate(dim_tables):
        alias = f"d{index}"
        map_alias = f"m{index}"
        sql_query += f"""
      LEFT JOIN {dimension}_map {map_alias} ON f.{dimension}_id = {map_alias}.{dimension}_id 
      LEFT JOIN {dimension} {alias} ON {map_alias}.{dimension}_id = {alias}.{dimension}_id
    """

    # Filtering
    sql_query += "WHERE 1=1 "
    for filter in filters:
        dim_alias = alias_map[filter["dimension"]]
        # Use a subquery to get the ID from the main dimension table based on name/alias
        filter_subquery = f"(SELECT {filter['dimension']}_id FROM {filter['dimension']} WHERE name = '{filter['member']}' OR alias = '{filter['member']}')"
        sql_query += f"AND {dim_alias}.{filter['dimension']}_id IN {filter_subquery} "

    print(sql_query)
    return sql_query




