from pydantic import BaseModel, constr
from typing import List, Literal, Optional, Dict

class TargetOptionsModel(BaseModel):
    primary_key: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None
    sequence_by: Optional[List[str]] = None
    mode: Literal['append', 'overwrite'] = 'overwrite'    
    apply_as_delete: Optional[str] = None
    checkpoint_location: Optional[constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$')] = None

class SourceModel(BaseModel):
    query: constr(pattern=r'^(?i)select .* from .*$', min_length=10)

class TargetModel(BaseModel):
    options: TargetOptionsModel
    catalog_name: Optional[str] = 'datalake'
    database_name: Optional[constr(pattern=r'^[a-zA-Z0-9_]+$')] = None
    database_path: Optional[constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$')] = None
    table_name: Optional[str] = None
    path: Optional[constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$')] = None
    format: Optional[Literal['delta']] = None
    sql_transformations: Optional[Dict[str, List[str]]] = None
    drop_columns: Optional[List[str]] = None

class StepModel(BaseModel):
    source: SourceModel  
    target: TargetModel    

class GoldSettingsModel(BaseModel):
    layer: str
    project: str
    subject: str
    table: str
    datalake_path: constr(pattern=r'^(s3a://[\w.-]+/?|/[A-Za-z0-9_/-]+)$') = None
    step_silver_to_gold: StepModel
    id: Optional[constr(pattern=r'^[a-zA-Z0-9_]+_[a-zA-Z0-9]+\.[a-zA-Z0-9_]+_[a-zA-Z0-9_]+$')] = None
    type_processing: Optional[constr(pattern=r'^(batch|streaming)$')] = 'batch'
    version: Optional[constr(pattern=r'^\d+\.\d+$')] = '1.0'
    active: Optional[constr(pattern=r'^(true|false)$')] = 'true'
    catalog_name: Optional[str] = 'datalake'
