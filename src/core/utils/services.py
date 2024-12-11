# src/services.py
import os
import psutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List
from src.converter import run_conversion
import logging

logger = logging.getLogger(__name__)

def calculate_optimal_workers(file_count: int, max_workers: Optional[int] = None) -> int:
    """Calculate optimal number of workers based on system resources and file count."""
    cpu_count = os.cpu_count() or 1
    available_memory = psutil.virtual_memory().available
    reserved_cpus = max(1, cpu_count // 4)
    max_cpus = cpu_count - reserved_cpus

    estimated_memory_per_process = 500 * 1024 * 1024
    max_processes_by_memory = available_memory // (estimated_memory_per_process * 2)

    optimal_workers = min(
        file_count,
        max_cpus,
        max_processes_by_memory,
        8
    )

    if max_workers is not None:
        optimal_max = optimal_workers
        if max_workers > optimal_max:
            logger.warning(
                f"Requested {max_workers} workers exceeds recommended maximum of {optimal_max}. "
                f"Limiting to {optimal_max} workers."
            )
            max_workers = optimal_max
        return max_workers

    return max(1, optimal_workers)





def process_files(input_paths: List[Path],
                  output: bool = False,
                  database: bool = False,
                  records: Optional[List[str]] = None,
                  max_workers: Optional[int] = None,
                  preprocessor_type: Optional[str] = None) -> None:
    """Process multiple STDF files in parallel with smart worker allocation."""
    workers = calculate_optimal_workers(len(input_paths), max_workers)
    logger.info(f"Processing {len(input_paths)} files using {workers} workers")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_path = {
            executor.submit(
                process_single_file,
                input_path,
                output,
                database,
                records,
                preprocessor_type
            ): input_path
            for input_path in input_paths
        }

        completed = 0
        for future in as_completed(future_to_path):
            input_path = future_to_path[future]
            try:
                future.result()
                completed += 1
                if completed % workers == 0:
                    logger.info(f"Processed {completed}/{len(input_paths)} files")
            except Exception as e:
                logger.error(f"Failed to process {input_path}: {str(e)}")


def process_single_file(input_file: Path,
                        output_file: Optional[Path] = None,
                        database_file: Optional[Path] = None,
                        records: Optional[List[str]] = None,
                        preprocessor_type: Optional[str] = None) -> None:
    """Process a single STDF file with corresponding output paths."""
    try:
        if output_file is None and database_file is None:
            run_conversion(str(input_file), None, None, records)
            return

        if output_file is True:
            output_file = input_file.with_suffix('.atdf')

        if database_file is True:
            database_file = input_file.with_suffix('.db')

        run_conversion(
            str(input_file),
            str(output_file) if output_file else None,
            str(database_file) if database_file else None,
            records,
            preprocessor_type
        )
        logger.info(f"Successfully processed {input_file}")

    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        raise