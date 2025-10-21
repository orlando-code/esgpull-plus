"""
Minimal enhanced File class that extends the base File class with additional metadata fields.
This approach is more focused and only adds what's necessary to capture additional metadata.
"""

from typing import Any, Dict, List, Optional
import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column
from esgpull.models import File as FileStatus
# from esgpull.models.file import FileDict as BaseFileDict
from esgpull.models.utils import find_str, find_int, get_local_path
from esgpull.models.base import Base
from esgpull.models import File


class EnhancedFile(Base):
    """
    Enhanced File class that extends the base File class with additional metadata fields.
    This is a minimal extension that only adds the necessary mapped columns and methods.
    """
    __tablename__ = "enhanced_file"
    
    # Base file fields (same as BaseFile)
    file_id: Mapped[str] = mapped_column(sa.String(255), unique=True)
    dataset_id: Mapped[str] = mapped_column(sa.String(255))
    master_id: Mapped[str] = mapped_column(sa.String(255))
    url: Mapped[str] = mapped_column(sa.String(255))
    version: Mapped[str] = mapped_column(sa.String(16))
    filename: Mapped[str] = mapped_column(sa.String(255))
    local_path: Mapped[str] = mapped_column(sa.String(255))
    data_node: Mapped[str] = mapped_column(sa.String(40))
    checksum: Mapped[str] = mapped_column(sa.String(64))
    checksum_type: Mapped[str] = mapped_column(sa.String(16))
    size: Mapped[int] = mapped_column(sa.BigInteger)
    status: Mapped[FileStatus] = mapped_column(sa.Enum(FileStatus), default=FileStatus.New)
    
    # Additional mapped columns for extended metadata
    variable: Mapped[str] = mapped_column(sa.String(255), default="")
    mip_era: Mapped[str] = mapped_column(sa.String(50), default="")
    institution_id: Mapped[str] = mapped_column(sa.String(100), default="")
    source_id: Mapped[str] = mapped_column(sa.String(100), default="")
    experiment_id: Mapped[str] = mapped_column(sa.String(100), default="")
    member_id: Mapped[str] = mapped_column(sa.String(100), default="")
    table_id: Mapped[str] = mapped_column(sa.String(50), default="")
    grid: Mapped[str] = mapped_column(sa.String(100), default="")
    grid_label: Mapped[str] = mapped_column(sa.String(100), default="")
    nominal_resolution: Mapped[str] = mapped_column(sa.String(50), default="")
    creation_date: Mapped[str] = mapped_column(sa.String(50), default="")
    title: Mapped[str] = mapped_column(sa.String(500), default="")
    instance_id: Mapped[str] = mapped_column(sa.String(100), default="")
    datetime_start: Mapped[str] = mapped_column(sa.String(50), default="")
    datetime_end: Mapped[str] = mapped_column(sa.String(50), default="")
    citation_url: Mapped[str] = mapped_column(sa.String(500), default="")
    cf_standard_name: Mapped[str] = mapped_column(sa.String(255), default="")
    variable_long_name: Mapped[str] = mapped_column(sa.String(500), default="")
    frequency: Mapped[str] = mapped_column(sa.String(50), default="")
    realm: Mapped[str] = mapped_column(sa.String(100), default="")
    activity_id: Mapped[str] = mapped_column(sa.String(100), default="")
    variant_label: Mapped[str] = mapped_column(sa.String(100), default="")
    sub_experiment_id: Mapped[str] = mapped_column(sa.String(100), default="")
    
    # Additional useful fields
    project: Mapped[str] = mapped_column(sa.String(100), default="")
    product: Mapped[str] = mapped_column(sa.String(100), default="")
    domain: Mapped[str] = mapped_column(sa.String(100), default="")
    time_frequency: Mapped[str] = mapped_column(sa.String(50), default="")
    variable_id: Mapped[str] = mapped_column(sa.String(100), default="")
    standard_name: Mapped[str] = mapped_column(sa.String(255), default="")
    long_name: Mapped[str] = mapped_column(sa.String(500), default="")
    units: Mapped[str] = mapped_column(sa.String(100), default="")
    cell_methods: Mapped[str] = mapped_column(sa.String(500), default="")
    cell_measures: Mapped[str] = mapped_column(sa.String(500), default="")
    type: Mapped[str] = mapped_column(sa.String(100), default="")
    processing_level: Mapped[str] = mapped_column(sa.String(100), default="")
    tracking_id: Mapped[str] = mapped_column(sa.String(100), default="")
    latest: Mapped[str] = mapped_column(sa.String(10), default="")
    replica: Mapped[str] = mapped_column(sa.String(10), default="")
    retracted: Mapped[str] = mapped_column(sa.String(10), default="")
    index_node: Mapped[str] = mapped_column(sa.String(100), default="")
    facets: Mapped[str] = mapped_column(sa.String(1000), default="")

    def _as_bytes(self) -> bytes:
        """Generate bytes for SHA computation."""
        self_tuple = (self.file_id, self.checksum)
        return str(self_tuple).encode()

    def compute_sha(self) -> None:
        """Compute SHA for this file."""
        Base.compute_sha(self)

    @classmethod
    def serialize(cls, source: dict) -> 'EnhancedFile':
        """
        Enhanced serialize method that captures all available metadata from ESGF search results.
        This is the key method that extracts all the rich metadata fields.
        """
        # Extract base file fields (same as original File.serialize)
        # Handle cases where fields might be missing or in different formats
        dataset_id = find_str(source.get("dataset_id", "")).partition("|")[0]
        filename = find_str(source.get("title", ""))
        url = find_str(source.get("url", "")).partition("|")[0]
        url = url.replace("http://", "https://")
        data_node = find_str(source.get("data_node", ""))
        checksum = find_str(source.get("checksum", ""))
        checksum_type = find_str(source.get("checksum_type", ""))
        
        # Handle size field more robustly
        try:
            size = find_int(source.get("size", 0))
        except (ValueError, TypeError):
            size = 0
        file_id = ".".join([dataset_id, filename])
        
        # Handle dataset_id parsing more robustly
        try:
            dataset_master, version = dataset_id.rsplit(".", 1)
        except ValueError:
            # If no version found, use a default
            dataset_master = dataset_id
            version = None
        
        master_id = ".".join([dataset_master, filename])
        
        # Create a simple local path without relying on directory_format_template_
        # This is a fallback when the template is not available
        # TODO: It's unclear what role this plays in the original code but retaining similar logic for now
        try:
            local_path = get_local_path(source, version)
        except KeyError:
            # Fallback: create a simple path structure
            local_path = f"{dataset_master}/{filename}"
        
        # Create the enhanced file with base fields
        result = cls(
            file_id=file_id,
            dataset_id=dataset_id,
            master_id=master_id,
            url=url,
            version=version,
            filename=filename,
            local_path=local_path,
            data_node=data_node,
            checksum=checksum,
            checksum_type=checksum_type,
            size=size,
        )
        
        # Extract and set all extended metadata fields
        cls._populate_extended_metadata(result, source)
        
        result.compute_sha()
        return result
    
    @classmethod
    def _populate_extended_metadata(cls, file_obj: 'EnhancedFile', source: dict) -> None:
        """
        Populate all extended metadata fields from the source dictionary.
        This method safely extracts all available metadata fields.
        """
        # Define field mappings - some fields might have different names in the source
        field_mappings = {
            'variable': 'variable',
            'mip_era': 'mip_era',
            'institution_id': 'institution_id',
            'source_id': 'source_id',
            'experiment_id': 'experiment_id',
            'member_id': 'member_id',
            'table_id': 'table_id',
            'grid': 'grid',
            'grid_label': 'grid_label',
            'nominal_resolution': 'nominal_resolution',
            'creation_date': 'creation_date',
            'title': 'title',
            'instance_id': 'instance_id',
            'datetime_start': 'datetime_start',
            'datetime_end': 'datetime_end',
            'citation_url': 'citation_url',
            'cf_standard_name': 'cf_standard_name',
            'variable_long_name': 'variable_long_name',
            'frequency': 'frequency',
            'realm': 'realm',
            'activity_id': 'activity_id',
            'variant_label': 'variant_label',
            'sub_experiment_id': 'sub_experiment_id',
            'project': 'project',
            'product': 'product',
            'domain': 'domain',
            'time_frequency': 'time_frequency',
            'variable_id': 'variable_id',
            'standard_name': 'standard_name',
            'long_name': 'long_name',
            'units': 'units',
            'cell_methods': 'cell_methods',
            'cell_measures': 'cell_measures',
            'type': 'type',
            'processing_level': 'processing_level',
            'tracking_id': 'tracking_id',
            'latest': 'latest',
            'replica': 'replica',
            'retracted': 'retracted',
            'index_node': 'index_node',
            'facets': 'facets',
        }
        
        # Extract all available fields
        for attr_name, source_key in field_mappings.items():
            if source_key in source:
                value = source[source_key]
                if isinstance(value, (list, tuple)):
                    # Handle list/tuple values by joining them
                    value = "|".join(str(v) for v in value)
                elif value is not None:
                    value = str(value)
                else:
                    value = ""
                
                setattr(file_obj, attr_name, value)

    def asdict(self) -> Dict[str, Any]:
        """Enhanced asdict method that includes all metadata fields."""
        result = {
            # Base fields
            'file_id': self.file_id,
            'dataset_id': self.dataset_id,
            'master_id': self.master_id,
            'url': self.url,
            'version': self.version,
            'filename': self.filename,
            'local_path': self.local_path,
            'data_node': self.data_node,
            'checksum': self.checksum,
            'checksum_type': self.checksum_type,
            'size': self.size,
            'status': self.status.name if self.status else "",
        }
        
        # Add all extended metadata fields
        extended_fields = [
            'variable', 'mip_era', 'institution_id', 'source_id', 'experiment_id',
            'member_id', 'table_id', 'grid', 'grid_label', 'nominal_resolution',
            'creation_date', 'title', 'instance_id', 'datetime_start', 'datetime_end',
            'citation_url', 'cf_standard_name', 'variable_long_name', 'frequency',
            'realm', 'activity_id', 'variant_label', 'sub_experiment_id',
            'project', 'product', 'domain', 'time_frequency', 'variable_id',
            'standard_name', 'long_name', 'units', 'cell_methods', 'cell_measures',
            'type', 'processing_level', 'tracking_id', 'latest', 'replica',
            'retracted', 'index_node', 'facets'
        ]
        
        for field in extended_fields:
            value = getattr(self, field, "")
            if value:  # Only include non-empty values
                result[field] = value
        
        return result

    @classmethod
    def fromdict(cls, source: Dict[str, Any]) -> 'EnhancedFile':
        """Enhanced fromdict method that handles all metadata fields."""
        # Extract base fields
        base_fields = {
            'file_id': source.get('file_id', ''),
            'dataset_id': source.get('dataset_id', ''),
            'master_id': source.get('master_id', ''),
            'url': source.get('url', ''),
            'version': source.get('version', ''),
            'filename': source.get('filename', ''),
            'local_path': source.get('local_path', ''),
            'data_node': source.get('data_node', ''),
            'checksum': source.get('checksum', ''),
            'checksum_type': source.get('checksum_type', ''),
            'size': source.get('size', 0),
        }
        
        # Create the enhanced file
        result = cls(**base_fields)
        
        # Set status if provided
        if 'status' in source:
            result.status = FileStatus(source.get('status', '').lower())
        
        # Set all extended metadata fields
        extended_fields = [
            'variable', 'mip_era', 'institution_id', 'source_id', 'experiment_id',
            'member_id', 'table_id', 'grid', 'grid_label', 'nominal_resolution',
            'creation_date', 'title', 'instance_id', 'datetime_start', 'datetime_end',
            'citation_url', 'cf_standard_name', 'variable_long_name', 'frequency',
            'realm', 'activity_id', 'variant_label', 'sub_experiment_id',
            'project', 'product', 'domain', 'time_frequency', 'variable_id',
            'standard_name', 'long_name', 'units', 'cell_methods', 'cell_measures',
            'type', 'processing_level', 'tracking_id', 'latest', 'replica',
            'retracted', 'index_node', 'facets'
        ]
        
        for field in extended_fields:
            if field in source:
                setattr(result, field, source[field])
        
        return result

    def clone(self, compute_sha: bool = True) -> 'EnhancedFile':
        """Enhanced clone method that copies all metadata fields."""
        result = EnhancedFile(
            file_id=self.file_id,
            dataset_id=self.dataset_id,
            master_id=self.master_id,
            url=self.url,
            version=self.version,
            filename=self.filename,
            local_path=self.local_path,
            data_node=self.data_node,
            checksum=self.checksum,
            checksum_type=self.checksum_type,
            size=self.size,
            status=self.status,
        )
        
        # Copy all extended metadata fields
        extended_fields = [
            'variable', 'mip_era', 'institution_id', 'source_id', 'experiment_id',
            'member_id', 'table_id', 'grid', 'grid_label', 'nominal_resolution',
            'creation_date', 'title', 'instance_id', 'datetime_start', 'datetime_end',
            'citation_url', 'cf_standard_name', 'variable_long_name', 'frequency',
            'realm', 'activity_id', 'variant_label', 'sub_experiment_id',
            'project', 'product', 'domain', 'time_frequency', 'variable_id',
            'standard_name', 'long_name', 'units', 'cell_methods', 'cell_measures',
            'type', 'processing_level', 'tracking_id', 'latest', 'replica',
            'retracted', 'index_node', 'facets'
        ]
        
        for field in extended_fields:
            value = getattr(self, field, "")
            setattr(result, field, value)
        
        if compute_sha:
            result.compute_sha()
        else:
            result.sha = self.sha
        
        return result


def create_enhanced_file_from_search_result(search_result: dict) -> EnhancedFile:
    """Create an EnhancedFile from a search result dictionary."""
    return EnhancedFile.serialize(search_result)


def create_enhanced_files_from_search_results(search_results: List[dict]) -> List[EnhancedFile]:
    """Create a list of EnhancedFile objects from search result dictionaries."""
    return [create_enhanced_file_from_search_result(result) for result in search_results]


def convert_base_files_to_enhanced(base_files: list[File], metadata: dict = None) -> list[EnhancedFile]:
    """Convert a list of base File objects to EnhancedFile objects."""
    enhanced_files = []
    
    for base_file in base_files:
        enhanced_file = EnhancedFile.fromdict(dict(base_file.asdict(), **metadata.get(base_file.file_id, {}) if metadata else {}))
        enhanced_files.append(enhanced_file)
    return enhanced_files