"""
Extended models for esgpullplus functionality.
These classes extend the base esgpull models with additional fields for better filtering and analysis.
"""

from typing import NotRequired
from typing_extensions import TypedDict

from esgpull.models import File as BaseFile, FileStatus
from esgpull.models.file import FileDict as BaseFileDict


class ExtendedFileDict(TypedDict):
    """Extended FileDict with additional metadata fields for better filtering."""
    # Base fields from upstream
    file_id: str
    dataset_id: str
    master_id: str
    url: str
    version: str
    filename: str
    local_path: str
    data_node: str
    checksum: str
    checksum_type: str
    size: int
    status: NotRequired[str]
    
    # Extended fields for esgpullplus
    variable: str
    mip_era: str
    institution_id: str
    source_id: str
    experiment_id: str
    member_id: str
    table_id: str
    grid: str
    grid_label: str
    nominal_resolution: str
    creation_date: str
    title: str
    instance_id: str
    datetime_start: str
    datetime_end: str
    citation_url: str


class ExtendedFile(BaseFile):
    """Extended File model with additional metadata fields."""
    
    # Extended fields for esgpullplus
    variable: str = ""
    mip_era: str = ""
    institution_id: str = ""
    source_id: str = ""
    experiment_id: str = ""
    member_id: str = ""
    table_id: str = ""
    grid: str = ""
    grid_label: str = ""
    nominal_resolution: str = ""
    creation_date: str = ""
    title: str = ""
    instance_id: str = ""
    datetime_start: str = ""
    datetime_end: str = ""
    citation_url: str = ""
    
    def to_extended_dict(self) -> ExtendedFileDict:
        """Convert to ExtendedFileDict for DataFrame operations."""
        return ExtendedFileDict(
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
            status=self.status.value if self.status else "",
            variable=self.variable,
            mip_era=self.mip_era,
            institution_id=self.institution_id,
            source_id=self.source_id,
            experiment_id=self.experiment_id,
            member_id=self.member_id,
            table_id=self.table_id,
            grid=self.grid,
            grid_label=self.grid_label,
            nominal_resolution=self.nominal_resolution,
            creation_date=self.creation_date,
            title=self.title,
            instance_id=self.instance_id,
            datetime_start=self.datetime_start,
            datetime_end=self.datetime_end,
            citation_url=self.citation_url,
        )
    
    @classmethod
    def from_base_file(cls, base_file: BaseFile, **extended_fields) -> "ExtendedFile":
        """Create ExtendedFile from base File object."""
        extended_file = cls()
        
        # Copy base fields
        extended_file.file_id = base_file.file_id
        extended_file.dataset_id = base_file.dataset_id
        extended_file.master_id = base_file.master_id
        extended_file.url = base_file.url
        extended_file.version = base_file.version
        extended_file.filename = base_file.filename
        extended_file.local_path = base_file.local_path
        extended_file.data_node = base_file.data_node
        extended_file.checksum = base_file.checksum
        extended_file.checksum_type = base_file.checksum_type
        extended_file.size = base_file.size
        extended_file.status = base_file.status
        
        # Set extended fields
        for field, value in extended_fields.items():
            if hasattr(extended_file, field):
                setattr(extended_file, field, value)
        
        return extended_file


# Convenience functions for working with extended models
def convert_base_files_to_extended(base_files: list[BaseFile], metadata: dict = None) -> list[ExtendedFile]:
    """Convert a list of base File objects to ExtendedFile objects."""
    extended_files = []
    
    for base_file in base_files:
        # Extract metadata if provided
        file_metadata = metadata.get(base_file.file_id, {}) if metadata else {}
        extended_file = ExtendedFile.from_base_file(base_file, **file_metadata)
        extended_files.append(extended_file)
    
    return extended_files


def create_extended_file_dict(file_data: dict) -> ExtendedFileDict:
    """Create ExtendedFileDict from raw file data."""
    return ExtendedFileDict(
        file_id=file_data.get("file_id", ""),
        dataset_id=file_data.get("dataset_id", ""),
        master_id=file_data.get("master_id", ""),
        url=file_data.get("url", ""),
        version=file_data.get("version", ""),
        filename=file_data.get("filename", ""),
        local_path=file_data.get("local_path", ""),
        data_node=file_data.get("data_node", ""),
        checksum=file_data.get("checksum", ""),
        checksum_type=file_data.get("checksum_type", ""),
        size=file_data.get("size", 0),
        status=file_data.get("status", ""),
        variable=file_data.get("variable", ""),
        mip_era=file_data.get("mip_era", ""),
        institution_id=file_data.get("institution_id", ""),
        source_id=file_data.get("source_id", ""),
        experiment_id=file_data.get("experiment_id", ""),
        member_id=file_data.get("member_id", ""),
        table_id=file_data.get("table_id", ""),
        grid=file_data.get("grid", ""),
        grid_label=file_data.get("grid_label", ""),
        nominal_resolution=file_data.get("nominal_resolution", ""),
        creation_date=file_data.get("creation_date", ""),
        title=file_data.get("title", ""),
        instance_id=file_data.get("instance_id", ""),
        datetime_start=file_data.get("datetime_start", ""),
        datetime_end=file_data.get("datetime_end", ""),
        citation_url=file_data.get("citation_url", ""),
    )
