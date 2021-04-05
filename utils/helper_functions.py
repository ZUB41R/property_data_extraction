import os
import pickle
import shutil
import logging
import glob

# we need these to check if an image is broken and try to fix
# from PIL import Image
import base64

# Serialisation Functions
def loadpickle(name):
    try:
        if os.path.split(name)[1].split('.')[-1] == "bin":
            filename = name
        else:
            print(f"Watch out for the extension!")
            filename = name + ".bin"
        with open(filename, 'rb') as f:
            return pickle.load(f)

    except (TypeError, FileNotFoundError) as e:
        return None


def dumppickle(name, something):
    try:
        filename = name + ".bin"
        with open(filename, 'wb') as f:
            return pickle.dump(something, f)
    except:
        return None


def droppickle(name):
    try:
        filename = name + ".bin"
        os.remove(filename)
        return True
    except FileNotFoundError:
        return False


# Directory Functions
def makedir(directory):
    try:
        if not os.path.isdir(directory):
            os.makedirs(directory)
            return True
        else:
            return True
    except:
        print("Could not make directory:" + str(directory))
        return False


def deletedir(directory):
    try:
        if os.path.exists(directory):
            shutil.rmtree(directory)

        return os.path.exists(directory)

    except:
        print("Could not remove directory:" + str(directory))
        return False


# File Functions
def isfile(pathtofile):
    try:
        return os.path.isfile(pathtofile)
    except:
        print("Could not determine if is file:" + str(pathtofile))
        return False


def isfilebiggerthan(pathtofile, size_in_bytes):
    try:
        # Size is less than 10KB!
        return os.stat(pathtofile).st_size >= size_in_bytes
    except:
        print("Could not verify file bigger than:", size_in_bytes, " at: ", str(pathtofile))
        return False


def verify_file(pathtofile, criteria_alias):
    try:
        success = isfile(pathtofile)

        if criteria_alias == "cut_white_image":
            success = isfilebiggerthan(pathtofile, 10000)

        return success
    except:
        print("Could not verify file matches criteria:", criteria_alias, " at: ", str(pathtofile))
        return False


def confirm_jpeg_read(filename) :
    """ Takes a filename of a jpg, if it can't be read tries to fix.
    Returns a boolean to indicate if the filename is a valid jpg. If 
    a file is successfully corrected, moves the original to .corrupted and
    saves the fixed version in its place"""
    
    good = True

    try :
        _ = Image.open(filename)
    except :
        good = False
    
        # read in the image as bytes
        with open(filename, "rb") as imageFile:
            image_bytes = imageFile.read()
    
        # replace non jpg characters introduced by server
        image_bytes = image_bytes.decode().replace(' ', '+').replace("\\r\\n", '').encode()
        imgdata = base64.b64decode(image_bytes)

        # write to a new file
        filename_corrected = filename + '.corrected.jpg'
        with open(filename_corrected, 'wb') as f:
            f.write(imgdata)
            f.close()

        try :
            # if the image is fixed keep the corrected and rename
            _ = Image.open(filename_corrected)
            os.rename(filename, filename + '.corrupted')
            os.rename(filename_corrected, filename)
            good = True
        except :
            os.remove(filename_corrected)

    return good

def read_file(file_path):
    with open(file_path, 'rb') as fp:
        return fp.read()


def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def set_logger(config, logger):
    # argument logger has been initiated already with respective module
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s: [%(levelname)s] [%(name)s] (%(threadName)-10s) %(message)s')
    
    file_handler = logging.FileHandler(config.log_path)
    file_handler.setFormatter(formatter)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger

def get_latest_file(storage_dir):
    list_of_files = glob.glob(f"{storage_dir}/*")
    latest_file_read = None
    latest_file_path=None
    try:
        latest_file_path = max(list_of_files, key=os.path.getctime)
    except Exception as e:
        print(f"Error: {e}")
    
    if latest_file_path is not None:
        if latest_file_path.split('.')[-1] == 'bin': 
            latest_file_read = loadpickle(latest_file_path)
        elif latest_file_path.split('.')[-1] == 'log':
            latest_file_read = read_file(latest_file_path)

    else:
        print(f"The latest file does not exists")

    return latest_file_read, latest_file_path
