import boto3
import urllib3
# import the builtin time module
import time
import pprint


urllib3.disable_warnings()


def boto3connection(access_key, secret_key, bucket_name, url):
    host = url
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, endpoint_url=host, verify=False)
    return s3


def get_blocks(client, bucket_name):
    objects = client.list_objects(Bucket=bucket_name)

    # Pretty Print a Dictionary using pprint
    # pprint.pprint(objects)

    # Filter only directories that looks like prometheus TSDB-block.
    # Block names look like 26 alphanumeric chars plus a trailing slash (e.g. '01F4WJWD8RKQW0F848G53VZNVM/')
    return list(it for it in objects['Contents'] if len(it['Key']) == 27)


def check_if_empty(block, client, bucket_name):
    delimit = block['Key'] + "meta.json"
    print(delimit)
    list_result = client.list_objects_v2(Delimiter='/', Bucket=bucket_name, Prefix=delimit)['KeyCount']
    print(list_result)
    return list_result


def delete_block(bucket, block_name, client):
    object_names_to_delete = []
    for folder in client.list_objects(Bucket=bucket, Prefix=block_name)['Contents']:
        print(folder['Key'])
        object_names_to_delete .append(folder['Key'])
    object_names_to_delete = sorted(object_names_to_delete, reverse=True)
    print(object_names_to_delete)
    if len(object_names_to_delete) == 0:
        print(f"Block {block_name} does not exist in {bucket} bucket.")
        return f"Block {block_name} does not exist in {bucket} bucket.\n"

    # client.remove_objects is not working properly with our S3 implementation, so we use remove_object in loop instead
    for object_name in object_names_to_delete:
        print(f"Deleting {object_name}")
        response = client.delete_object(
            Bucket=bucket,
            Key=object_name,
        )
        print(response['ResponseMetadata']['HTTPStatusCode'])
        print(f"Block {object_name} successfully deleted.")
    return f"Block {block_name} successfully deleted.\n"


def clean_empty_blocks(client, bucket_name):
    blocks = get_blocks(client, bucket_name)
    blocks_count = len(blocks)
    non_empty_blocks = []

    for i, block in enumerate(blocks):
        # print(block)
        if not check_if_empty(block, client, bucket_name):
            print(f"[{i+1}/{blocks_count}] Deleting empty block {block['Key']}")
            delete_block(bucket_name, block['Key'], client)
        else:
            print(f"[{i+1}/{blocks_count}] Discovered block: {block['Key']}")
            non_empty_blocks.append(block)

    non_empty_blocks_count = len(non_empty_blocks)
    print(
        f"Cleanup finished. Removed {blocks_count - non_empty_blocks_count} blocks."
        f"Discovered {non_empty_blocks_count} non-empty blocks.")


# variables
x = 'access_key'
y = 'secret_key'
z = 'bucket_name'
t = 'https://url'

if __name__ == "__main__":
    while True:
        # Grab Current Time Before Running the Code
        start = time.time()
        # initialize connection
        connection = boto3connection(x, y, z, t)
        # run function to search and delete for empty directories
        clean_empty_blocks(connection, z)
        # Grab Current Time After Running the Code
        end = time.time()
        # Subtract Start Time from The End Time
        total_time = end - start
        print("\n" + str(total_time) + "seconds to execute")
        time.sleep(5)
