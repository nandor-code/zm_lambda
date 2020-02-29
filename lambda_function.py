# Door Bell Lambda Handler
# Nandor Szots
# nandor@ntsj.com

import json
import os
import urllib.parse
import boto3
import urllib3
import hashlib
import cv2

colors = {'blue': (255, 0, 0), 'green': (0, 255, 0), 'red': (0, 0, 255), 
          'yellow': (0, 255, 255),'magenta': (255, 0, 255), 
          'cyan': (255, 255, 0), 'white': (255, 255, 255), 'black': (0, 0, 0), 
          'gray': (125, 125, 125), 'dark_gray': (50, 50, 50), 'light_gray': (220, 220, 220)}

http = urllib3.PoolManager()

print('Loading function')

s3          = boto3.client('s3')
rekognition = boto3.client('rekognition')
dynamodb    = boto3.client('dynamodb')

ACCESS_TOKEN    = os.environ['ACCESS_TOKEN']    # Slack OAuth access token from environment variables
SLACK_CHANNEL   = os.environ['SLACK_CHANNEL']   # Slack Channel to post to
COLLECTION_NAME = os.environ['COLLECTION_NAME'] # Rekognition Collection Name

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    #Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']

    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    img_data = response['Body'].read()
    
    hash = hashlib.md5(img_data)
    #print(hash.hexdigest())
    
    # lambda has an annoying habit of being fired multiple times for the same event (due to timeouts etc)
    # net-net this means your event might end up succeeding late and multiple times which is annoying and expensive
    # since this isnt mission-critical we just store the md5 hash of every image we have seen and never process the same 
    # request twice.
    if have_proccessed_hash( str( hash.hexdigest() ) ):
        print ("Already processed this image hash.")
        return True;
    
    update_proccessed_hash(str(hash.hexdigest()))
    
    #print ("DOING OBJ DETECTION")
    msg, objects = detect_objs( img_data )
    msg += "\n"
    
    #print ("DOING FACE DETECTION")
    msg, person, faces = detect_faces( img_data, msg )
    
    #print ("POSTING TO SLACK")
    img_data = annotate_img(img_data, person, faces, objects)
    post_image( SLACK_CHANNEL, msg, img_data )

    #print ("DONE")
    return True
    
def update_proccessed_hash(hash):
    print ("Updating DB with hash: " + hash)
    response = dynamodb.put_item(
        TableName='processed_image_hashes',
        Item={
            'hash': {'S': hash}
            }
        )
    print(response)
    
def have_proccessed_hash(hash):
    proc_hash = dynamodb.get_item(
                TableName='processed_image_hashes',
                Key={'hash': {'S': hash}}
                )
                
    if 'Item' in proc_hash:
        return True
    else:
        return False
    
def detect_faces(image_bytes, msg):
    faces = {}
    person = "Unknown Person"
    ret = msg + 'People: '
    try:
        response = rekognition.search_faces_by_image(
            CollectionId='ntsj_collection',
            Image={
                'Bytes': image_bytes,
            }
        )
        
    except Exception as e:
        print(e)
        ret += 'No faces found.'
        #print('Unable to detect labels for image.')
        return ret, person, faces
        
    #print(response)
    
    # Just take the first face found and print it.  It is also possible to print every face found but 
    # I seemed to get the same person multiple times that way and I havent debugged why yet.
    # I think overlaying boxes with names on the image would be a cool way to go and might be a great v2 feature.
    faces = response['FaceMatches']
    if len(faces) > 0:
        match = faces[0]
        face = dynamodb.get_item(
            TableName=COLLECTION_NAME,  
            Key={'RekognitionId': {'S': match['Face']['FaceId']}}
            )
            
        if 'Item' in face:
            ret += (face['Item']['FullName']['S']) + " (" + str(int(match['Face']['Confidence'])) + "%) "
            person = face['Item']['FullName']['S']
        else:
            ret += ('Unknown Person')                
    else:
        ret += 'No faces found.'

    return ret, person, response
    
def detect_objs(image_bytes):
    ret = ""
    labels = {}
    try:
        response = rekognition.detect_labels(
            Image={
                'Bytes': image_bytes,
            },
            MinConfidence=80.0
        )
        
    except Exception as e:
        print(e)
        ret += "None objects found."
        print('Unable to detect labels for image.')
        return ret, labels
    
    #print( response )
    labels = response['Labels']
    for label in labels:
        if len(ret) > 0:
            ret += ", " + label['Name']
        else:
            ret += "Objects: " + label['Name']
        
    return ret, response
    
def post_image(channel, msg, img):
    """ Posts img to Slack channel via Slack API.
    Args:
        channel (string): Channel, private group, or IM channel to send message to. Can be an encoded ID, or a name.
        msg: text to go along with the image. 
        img: image data to post to channel
    Returns:
        (None)
    """
    
    url = 'https://slack.com/api/files.upload'

    files = { 'file': img }
    values = { 'token': ACCESS_TOKEN,
               'channels': channel }

    r = http.request( 'POST',
                      url,
                      fields={
                        'token': ACCESS_TOKEN,
                        'channels': channel,
                        'initial_comment': msg,
                        'file': ( 'Camera.jpg', img, 'image/jpeg' )
                      })
    
    print( r.data )

def annotate_img(img_data, person, faces, objects):
    if( len(faces) == 0 ):
        return img_data
    
    # More than anything I wish I could do this in memory but cv2 apparently sucks 
    # and has a really hard time reading from memory so here we are...
    tmp_filename='/tmp/my_image.jpg'
    f = open(tmp_filename, "wb")
    f.write(img_data)
    f.close()
    
    src_image  = cv2.imread(tmp_filename)
    
    draw_face_data( src_image, person, faces )

    draw_object_data( src_image, objects )

    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
    result, img_data = cv2.imencode('.jpg', src_image, encode_param)
    
    return img_data
    
def draw_object_data( img, objects ):
    height = img.shape[0]
    width  = img.shape[1]
    
    labels = objects['Labels']
    for label in labels:
        if label["Confidence"] > 95:
            for inst in label["Instances"]:
                if inst["Confidence"] > 95:
                    box = inst["BoundingBox"]
                    if box:
                        left, top, right, bott = get_bounding_box( box, width, height )
                        draw_box_with_text( img, left, top, right, bott, label["Name"], colors["magenta"], colors["magenta"] )
                

def draw_face_data(img, person, faces):
    height = img.shape[0]
    width  = img.shape[1]
    
    box = faces["SearchedFaceBoundingBox"]
    
    left, top, right, bott = get_bounding_box( box, width, height )

    draw_box_with_text( img, left, top, right, bott, person, colors["green"], colors["green"] )

def draw_box_with_text(img, xa, xb, ya, yb, text, box_color, text_color ):
    # Create the bounding box.
    cv2.rectangle(img, (int(xa),int(xb)), (int(ya),int(yb)), box_color, 2)
    
    # Label it.
    cv2.putText(img, text, (int(xa), int(xb-20)), cv2.FONT_HERSHEY_SIMPLEX, 1, text_color, 2)

def get_bounding_box( box, width, height ):
    left  = box["Left"] * width
    top   = box["Top"]  * height
    right = left + ( box["Width"]  * width )
    bott  = top  + ( box["Height"] * height )
    
    return left, top, right, bott
