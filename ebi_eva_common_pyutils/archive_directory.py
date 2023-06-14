#!/usr/bin/env python
import gzip
import shutil
import tarfile
import os.path
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config
from retry import retry

logger = logging_config.get_logger(__name__)


def make_tarfile(output_filename, source_dir):
    logger.info(f'Create Final Tar file {output_filename}.')
    with tarfile.open(output_filename, "w") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    file_stats = os.stat(output_filename)
    logger.info(f'{output_filename} completed. File Size in Bytes is {file_stats.st_size}')


def is_compressed(file_path):
    compressed_ext = ['.gz', '.zip', '.bz', '.tbi', '.csi']
    for ext in compressed_ext:
        if file_path.lower().endswith(ext):
            return True
    return False


@retry(tries=5, delay=3, backoff=2, logger=logger)
def retriable_compress(src_file_path, dest_file_path):
    MEG = 2 ** 20
    with open(src_file_path, 'rb') as f_in:
        with gzip.open(dest_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out, length=16 * MEG)


def matches(name, patterns):
    return any((pattern for pattern in patterns if pattern in name))


@retry(tries=5, delay=3, backoff=2, logger=logger)
def retriable_remove(path):
    shutil.rmtree(path)


@retry(tries=5, delay=3, backoff=2, logger=logger)
def retryable_copy(src_file_path, dest_file_path, **kwargs):
    if os.path.exists(dest_file_path):
        os.remove(dest_file_path)
    shutil.copyfile(src_file_path, dest_file_path, **kwargs)


def archive_directory(source_dir, scratch_dir, destination_dir, filter_patterns=None):
    """
    Archive a directory by copying the data it contains to a scratch directory compressing all files that are not
    already compressed. Then it create a tar file to the destination_dir under the name of the original directory.
    """
    source_dir_name = os.path.basename(source_dir)
    logger.info(f'Archive {source_dir_name} from {source_dir}')
    parent_source_dir = os.path.dirname(source_dir)
    for base, dirs, files in os.walk(source_dir, topdown=True, followlinks=False):
        # Filter the downstream directory to
        filtered_dir = []
        for d in dirs:
            if matches(d, filter_patterns):
                logger.info(f'Ignore directory {d} because of filters: {filter_patterns}')
            else:
                filtered_dir.append(d)
        # modify dirs in place
        dirs[:] = filtered_dir
        src_basename = os.path.relpath(base, parent_source_dir)
        scratch_dest_dir = os.path.join(scratch_dir, src_basename)
        os.makedirs(scratch_dest_dir, exist_ok=True)
        for fname in files:
            src_file_path = os.path.join(base, fname)
            dest_file_path = os.path.join(scratch_dest_dir, fname)
            if matches(fname, filter_patterns):
                logger.info(f'Ignore file {src_file_path} because of filters: {filter_patterns}')
                continue
            if os.path.islink(src_file_path) or is_compressed(src_file_path):
                logger.info(f'Copy {src_file_path}')
                retryable_copy(src_file_path, dest_file_path, follow_symlinks=False)
            else:
                logger.info(f'Compress {src_file_path}')
                retriable_compress(src_file_path, dest_file_path + '.gz')
    final_tar_file = os.path.join(destination_dir, source_dir_name + '.tar')
    scratch_dir_archived = os.path.join(scratch_dir, source_dir_name)
    make_tarfile(final_tar_file, scratch_dir_archived)
    logger.info(f'Delete scratch folder {scratch_dir_archived}.')
    retriable_remove(scratch_dir_archived)
    logger.info(f'Scratch folder {scratch_dir_archived} deleted.')


def main():
    parser = ArgumentParser(description='Archive a directory by copying the data it contains to a scratch directory '
                                        'compressing all files that are not already compressed. Then it create a tar '
                                        'file to the destination_dir under the name of the original directory.')
    parser.add_argument('--source_dir', required=True, type=str,
                        help='base directory you want to archive. All sub directories will be included.')
    parser.add_argument('--destination_dir', required=True, type=str,
                        help='Directory where the archive should be placed at the end of the process.')
    parser.add_argument('--scratch_dir', required=True, type=str,
                        help='Directory where the archive will be constructed.')
    parser.add_argument('--filter_patterns', type=str, nargs='*', default=[],
                        help='keyword found in file and directory names that not be included to the archive.')
    args = parser.parse_args()

    logging_config.add_stdout_handler()
    archive_directory(args.source_dir, args.scratch_dir,  args.destination_dir, args.filter_patterns)


if __name__ == '__main__':
    main()
