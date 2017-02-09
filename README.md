# ceph2swift

Very simple pipline implementation to migrate existing keys from Ceph to Swift


Setup
You should create a virtual environment and install the requirements.

```
virtualenv env
source ./env/bin/activate
pip install requirements.txt
```

Credentials can be defined as environment variables.

```bash
export SRC_HOST=cephgw.alayacare.ca
export SRC_KEY_ID=<SRC_KEY>
export SRC_ACCESS_KEY=<SRC_ACCESS_KEY>
export DST_REGION=<DST_REGION>
export DST_HOST=<DST_HOST>
export DST_KEY_ID=<DST_KEY>
export DST_ACCESS_KEY=<DST_ACCESS_KEY>
```

Then you can run

```
source credentials.sh

./ceph2swift.py --src-bucket=alaya-demo3 --dst-bucket=alaya-testing

```
