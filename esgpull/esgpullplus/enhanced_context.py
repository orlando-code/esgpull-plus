"""
Simple enhanced context that converts File objects to EnhancedFile objects.
This is the most straightforward approach.
"""

from esgpull.context import Context as OriginalContext, ResultFiles
from esgpull.esgpullplus.enhanced_file import EnhancedFile
from esgpull.models import Query
from typing import Sequence, Any

class EnhancedContext:
    """
    Enhanced context wrapper that converts File objects to EnhancedFile objects.
    """
    
    def __init__(self):
        self._original_context = OriginalContext()
    
    def __getattr__(self, name):
        """Delegate all other attributes to the original context."""
        return getattr(self._original_context, name)
    
    def search(
        self,
        *queries: Query,
        file: bool,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        date_from: Any = None,
        date_to: Any = None,
        keep_duplicates: bool = True,
    ) -> Sequence[EnhancedFile | Any]:
        """
        Enhanced search method that returns EnhancedFile objects for file searches.
        """
        if file:
            # Patch the ResultFiles.process method to create EnhancedFile objects
            original_process = ResultFiles.process
            
            def enhanced_process_files(self):
                """Enhanced process method that creates EnhancedFile objects."""
                self.data = []
                if self.success:
                    for doc in self.json["response"]["docs"]:
                        try:
                            # Create EnhancedFile directly from raw search results
                            file = EnhancedFile.serialize(doc)
                            self.data.append(file)
                        except KeyError as exc:
                            from esgpull.tui import logger
                            logger.exception(exc)
                            fid = doc["instance_id"]
                            logger.warning(f"File {fid} has invalid metadata")
                            from rich.pretty import pretty_repr
                            logger.debug(pretty_repr(doc))
                self.processed = True
            
            # Temporarily patch the process method
            ResultFiles.process = enhanced_process_files
            
            try:
                # Use the original search method
                results = self._original_context.search(
                    *queries,
                    file=file,
                    hits=hits,
                    offset=offset,
                    max_hits=max_hits,
                    page_limit=page_limit,
                    date_from=date_from,
                    date_to=date_to,
                    keep_duplicates=keep_duplicates,
                )
                return results
            finally:
                # Restore the original process method
                ResultFiles.process = original_process
        else:
            # For datasets, use the original method
            return self._original_context.search(
                *queries,
                file=file,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                page_limit=page_limit,
                date_from=date_from,
                date_to=date_to,
                keep_duplicates=keep_duplicates,
            )

# Create a global instance
Context = EnhancedContext()
