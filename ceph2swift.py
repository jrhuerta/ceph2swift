#!/usr/bin/env python
import arrow
import argparse
import boto
import boto.s3.connection
import logging
import os
import signal
import time


class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if not default and envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required,
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


class Stage(object):
    items = None

    def __init__(self, items=None, **kwargs):
        self.items = items
        self.config = kwargs

    def configure(self, **kwargs):
        self.config.update(kwargs)

    def process(self, item):
        return item

    def before_process(self):
        pass

    def after_process(self):
        pass

    def __iter__(self):
        self.before_process()
        for item in self.items:
            try:
                yield self.process(item)
            except Exception as ex:
                print("SKIPPED: {}".format(ex.message))
        self.after_process()


class Pipeline(object):
    items = None
    start_time = 0
    exit = False

    def __init__(self, items, *stages, **kwargs):
        self.items = items
        self.config = kwargs
        for stage in stages:
            stage.configure(**kwargs)
            self.add(stage)

    def add(self, stage):
        stage.items = self.items
        self.items = stage

    def before_process(self):
        self.start_time = time.time()

    def after_process(self):
        print("*"*80)
        print("Time elapsed: {}".format(time.time() - self.start_time))
        pass

    def __call__(self):
        self.before_process()
        for _ in self.items:
            pass
        self.after_process()


class PrintFileInfo(Stage):

    def process(self, item):
        print('-'*80)
        print(item.name)
        print("SRC MD5: {}".format(item.etag[1:-1]))
        return item

    def after_process(self):
        print("*"*80)


class Filter(Stage):

    def __init__(self, name, filter, **kwargs):
        super(Filter, self).__init__(**kwargs)
        self.name = name
        self.filter = filter

    def process(self, item):
        if self.filter(item):
            raise RuntimeError("by {} filter.".format(self.name))
        return item


class S3Stage(Stage):

    @property
    def connection(self):
        try:
            return self.config['connection']
        except KeyError:
            raise RuntimeError('S3Stage: connection not configured.')

    @property
    def bucket_name(self):
        try:
            return self.config['bucket_name']
        except ValueError:
            raise RuntimeError('S3Stage: tenant name not configured.')

    @property
    def bucket(self):
        try:
            return self.connection.get_bucket(self.bucket_name)
        except Exception as ex:
            raise ex


class S3CreateFolderStructure(S3Stage):

    content_type = 'application/directory'
    last_modified = 'x-last-modified'
    existing_folders = None
    start_folder_count = 0

    def load_existing_folders(self):
        existing_folders = set()
        print('Preloading existing folders ')
        for key in self.bucket.list():
            key = self.bucket.get_key(key.name)
            if key.content_type == self.content_type:
                existing_folders.add(key.name)
        print('{} folders loaded.'.format(len(existing_folders)))
        return existing_folders

    def sub_folders(self, filename):
        folders = filename.split('/')[:-1]
        current_path = ''
        for folder in folders:
            current_path = current_path + folder + '/'
            yield current_path

    def create_folder(self, path, item):
        try:
            key = self.bucket.new_key(path)
            key.content_type = self.content_type
            key.set_metadata(self.last_modified,
                             arrow.get(item.last_modified).isoformat())
            key.set_contents_from_string('')
            self.existing_folders.add(path)
            print(path)
        except Exception as e:
            print("{}: {}".format(path, e.message))

    def before_process(self):
        self.existing_folders = self.load_existing_folders()
        self.start_folder_count = len(self.existing_folders)

    def after_process(self):
        print("{} folder(s) created.".format(
            len(self.existing_folders) - self.start_folder_count))

    def process(self, item):
        print('FOLDERS:')
        for folder in self.sub_folders(item.name):
            if folder in self.existing_folders:
                continue
            self.create_folder(folder, item)
        return item


class S3UploadFile(S3Stage):

    last_modified = 'x-last-modified'
    key_count = 0

    def process(self, item):
        key = self.bucket.get_key(item.name)
        if key:
            assert item.etag[1:-1] != key.etag[1:-1], "File already exists."

        key = self.bucket.new_key(item.name)
        key.set_metadata(self.last_modified,
                         arrow.get(item.last_modified).isoformat())
        key.set_contents_from_string(item.get_contents_as_string())

        key = self.bucket.get_key(item.name)

        print('DST MD5: {}'.format(key.etag[1:-1]))
        if item.etag[1:-1] != key.etag[1:-1]:
            print('WARNING: source and destination hash don\'t match.')
        else:
            self.key_count += 1
        return item

    def after_process(self):
        print("{} file(s) copied.".format(self.key_count))


def args_spec():
    parser = argparse.ArgumentParser(
        prog='ceph2s3.py',
        description='Optional arguments can be defined as '
                    'environment variables.')

    parser.add_argument('--src-bucket', type=str, required=True)
    parser.add_argument('--dst-bucket', type=str, required=True)

    parser.add_argument('--src-key-id', type=str, action=EnvDefault,
                        envvar='SRC_KEY_ID')
    parser.add_argument('--src-access-key', type=str, action=EnvDefault,
                        envvar='SRC_ACCESS_KEY')
    parser.add_argument('--src-host', type=str, action=EnvDefault,
                        envvar='SRC_HOST')

    parser.add_argument('--dst-key-id', type=str, action=EnvDefault,
                        envvar='DST_KEY_ID')
    parser.add_argument('--dst-access-key', type=str, action=EnvDefault,
                        envvar='DST_ACCESS_KEY')
    parser.add_argument('--dst-host', type=str, action=EnvDefault,
                        envvar='DST_HOST')
    parser.add_argument('--dst-region', type=str, action=EnvDefault,
                        envvar='DST_REGION')

    return parser


_exit_signal = False


def signal_handler(signal, frame):
    print('User pressed Ctrl-C!')
    global _exit_signal
    _exit_signal = True


def src_keys_generator(conn, bucket_name):
    for key in conn.get_bucket(bucket_name).list():
        if _exit_signal:
            raise StopIteration
        yield key


def main():
    parser = args_spec()
    args = parser.parse_args()

    src_connection = boto.connect_s3(
        aws_access_key_id=args.src_key_id,
        aws_secret_access_key=args.src_access_key,
        host=args.src_host,
        is_secure=True,
        calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )

    dst_connection = boto.s3.connect_to_region(
        args.dst_region,
        aws_access_key_id=args.dst_key_id,
        aws_secret_access_key=args.dst_access_key,
        host=args.dst_host
    )

    signal.signal(signal.SIGINT, signal_handler)

    p = Pipeline(src_keys_generator(src_connection, args.src_bucket))

    p.add(PrintFileInfo())
    p.add(Filter('exclude keys with \'default\' in the name',
                 lambda x: 'default' in x.name))

    p.add(S3CreateFolderStructure(connection=dst_connection,
                                  bucket_name=args.dst_bucket))
    p.add(Filter('exclude keys ending in \'/\'',
                 lambda x: x.name.endswith('/')))
    p.add(S3UploadFile(connection=dst_connection, bucket_name=args.dst_bucket))
    p()

if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG)
    main()

