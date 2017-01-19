#!/usr/bin/env python
import argparse
import functools
import boto
import boto.s3.connection
import logging
import os
import swiftclient


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


class Pipeline(object):
    def __init__(self, items=None, *stages):
        self._items = items
        for stage in stages:
            self.add(stage)

    def add(self, stage):
        self._items = stage(self._items)
        return self

    def before_run(self):
        pass

    def after_run(self):
        pass

    def run(self):
        self.before_run()
        for _ in self._items:
            pass
        self.after_run()


class Stage(object):
    def process(self, item):
        return item

    def before_process(self):
        pass

    def after_process(self):
        pass

    def __call__(self, items):
        self.before_process()
        for item in items:
            try:
                yield self.process(item)
            except Exception as ex:
                print("SKIPPED: {}".format(ex.message))
        self.after_process()


class S3KeyInfo(Stage):

    def process(self, item):
        print('-'*80)
        print(item.name)
        print("MD5:{}".format(item.etag[1:-1]))
        return item


class Filter(Stage):
    def __init__(self, name, filter):
        self.name = name
        self.filter = filter

    def __call__(self, items):
        for item in items:
            if self.filter(item):
                print("SKIPPED: by {} filter.".format(self.name))
                continue
            yield item


class SwiftStage(Stage):

    def __init__(self, connection, tenant):
        self.connection = connection
        self.tenant = tenant

    @property
    def container_name(self):
        return 'alaya-{}'.format(self.tenant)

    def __getattr__(self, item):
        method = getattr(self.connection, item)
        return functools.partial(method, self.container_name)


class SwiftCreateTenantContainer(SwiftStage):

    def before_process(self):
        try:
            self.put_container()
            print("{}: Created container".format(self.container_name))
        except Exception as ex:
            logging.error("{}: Unable to create bucket.")
            logging.exception(ex)
            raise


class SwiftCheckIfFileExists(SwiftStage):
    def before_process(self):
        #load all keys from swift to speed up checking if the file exists.
        pass

    def process(self, item):
        try:
            headers = self.head_object(item.name)
            assert item.etag[1:-1] != headers.get('etag'), "checksum verified."
        except swiftclient.ClientException as ex:
            if getattr(ex, 'http_status', None) != 404:
                raise
        return item


class SwiftCreateFolderStructure(SwiftStage):
    def __init__(self, *args, **kwargs):
        super(SwiftCreateFolderStructure, self).__init__(*args, **kwargs)
        self.existing_folders = set()

    def sub_folders(self, filename):
        folders = filename.split('/')[:-1]
        current_path = ''
        for folder in folders:
            current_path = current_path + ('/' if current_path else '') + folder
            yield current_path

    def create_folder(self, path):
        try:
            self.put_object(obj=path, contents=None,
                            content_type='application/directory')
            self.existing_folders.add(path)
            print('{}: folder created.'.format(path))
        except Exception as e:
            print(e.message)
            raise

    def process(self, item):
        for folder in self.sub_folders(item.name):
            if folder in self.existing_folders:
                continue
            self.create_folder(folder)
        return item


class SwiftUploadFile(SwiftStage):

    def process(self, item):
        self.put_object(
            item.name,
            contents=item.get_contents_as_string(),
            content_type=item.content_type
        )
        headers = self.head_object(item.name)
        assert headers.get('etag') == item.etag[1:-1], \
            'Checksum check failed.'
        print('Checksum check passed.')


def list_s3_keys(access_key_id, secret_access_key, host, tenant):
    try:
        conn = boto.connect_s3(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            host=host,
            is_secure=True,
            calling_format=boto.s3.connection.OrdinaryCallingFormat()
        )
        bucket = conn.lookup('alaya-{}'.format(tenant))
        assert bucket, "{}: tenant bucket not found.".format(tenant)
        for key in bucket.list():
            yield key
    except Exception as ex:
        logging.error('Unable to list S3 keys.')
        logging.error(ex.message)
        raise ex


def args_spec():
    parser = argparse.ArgumentParser(
        prog='ceph2swift.py',
        description='Optional arguments can be defined as '
                    'environment variables.')
    parser.add_argument('--ceph-key-id', type=str, action=EnvDefault,
                        envvar='CEPH_KEY_ID')
    parser.add_argument('--ceph-access-key', type=str, action=EnvDefault,
                        envvar='CEPH_ACCESS_KEY')
    parser.add_argument('--ceph-host', type=str, action=EnvDefault,
                        envvar='CEPH_HOST')

    parser.add_argument('--swift-auth-url', type=str, action=EnvDefault,
                        envvar='SWIFT_AUTH_URL')
    parser.add_argument('--swift-tenant-name', type=str, action=EnvDefault,
                        envvar='SWIFT_TENANT_NAME')
    parser.add_argument('--swift-user', type=str, action=EnvDefault,
                        envvar='SWIFT_USER')
    parser.add_argument('--swift-password', type=str, action=EnvDefault,
                        envvar='SWIFT_PASSWORD')

    parser.add_argument('--tenant', type=str, required=True)

    return parser


def main():
    parser = args_spec()
    args = parser.parse_args()

    conn = swiftclient.client.Connection(
        authurl=args.swift_auth_url,
        user=args.swift_user,
        key=args.swift_password,
        tenant_name=args.swift_tenant_name,
        auth_version=2
    )

    p = Pipeline(list_s3_keys(
        args.ceph_key_id,
        args.ceph_access_key,
        args.ceph_host,
        args.tenant
    ))

    p.add(S3KeyInfo())
    p.add(Filter('exclude keys with \'default\' in the name.',
                 lambda x: 'default' in x.name))
    p.add(SwiftCreateTenantContainer(connection=conn, tenant=args.tenant))
    p.add(SwiftCreateFolderStructure(connection=conn, tenant=args.tenant))
    p.add(Filter('exclude keys ending in \'\\\'',
                 lambda x: x.name.endswith('/')))
    p.add(SwiftCheckIfFileExists(connection=conn, tenant=args.tenant))
    p.add(SwiftUploadFile(connection=conn, tenant=args.tenant))

    p.run()

if __name__ == '__main__':
    main()
