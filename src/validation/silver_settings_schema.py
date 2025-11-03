from pydantic import BaseModel, constr, Field
from typing import List, Optional, Literal, Dict

class SourceOptionsModel(BaseModel):
    header: Optional[constr(pattern=r'^(true|false)$')] = None
    delimiter: Optional[str] = None
    quote: Optional[str] = None
    inferSchema: Optional[constr(pattern=r'^(true|false)$')] = 'true'

class TargetOptionsModel(BaseModel):
    primary_key: Optional[List[str]] = None
    sequence_by: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None
    apply_as_delete: Optional[str] = None
    mode: Literal['append', 'overwrite'] = 'overwrite'
    checkpoint_location: Optional[constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$')] = None

class SourceModel(BaseModel):
    format: Optional[str] = None
    path: Optional[constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$')] = None
    data_schema: Optional[Dict] = Field(default=None, alias="schema")
    options: Optional[SourceOptionsModel] = None

class TargetModel(BaseModel):
    catalog_name: Optional[str] = 'datalake'
    database_name: Optional[constr(pattern=r'^[a-zA-Z0-9_]+$')] = None
    database_path: Optional[constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$')] = None
    table_name: Optional[str] = None
    path: Optional[constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$')] = None
    format: Optional[Literal['delta']] = 'delta'
    options: Optional[TargetOptionsModel] = None
    sql_transformations: Optional[Dict[str, List[str]]] = None
    drop_columns: Optional[List[str]] = None

class StepModel(BaseModel):
    source: SourceModel
    target: TargetModel

class SilverSettingsModel(BaseModel):
    layer: str
    project: str
    subject: str
    table: str
    datalake_path: constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$') = None
    step_raw_to_bronze: StepModel
    step_bronze_to_silver: StepModel
    id: Optional[constr(pattern=r'^[a-zA-Z0-9_]+_[a-zA-Z0-9]+\.[a-zA-Z0-9_]+_[a-zA-Z0-9_]+$')] = None
    type_processing: Optional[constr(pattern=r'^(batch|streaming)$')] = 'batch'
    version: Optional[constr(pattern=r'^\d+\.\d+$')] = '1.0'
    active: Optional[constr(pattern=r'^(true|false)$')] = 'true'
    catalog_name: Optional[str] = 'datalake'