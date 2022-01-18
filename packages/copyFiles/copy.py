import pathlib
import time
from tracemalloc import start
from collections import deque


# function to check the function parameters are of write type or not
# paramsDict is the dict containing info to be checked by the function
# paramsDict = {
# "myVar" : (myVar , (str , byte)) , 
# }
# here the myVar variable will be check if it is str or bytes type , else TypeError will be raised
def checkParameters(functionName , paramsDict):

    def search(parmValue , typeOfParm):
        for i in typeOfParm:
            if(type(parmValue) == i):
                return True

        return False


    for parmName , keyValue in paramsDict.items():

        parmValue , typeOfParm = keyValue

        if(not(search(parmValue , typeOfParm))):
            raise TypeError(f"{parmName} parameter expected to any of {typeOfParm} type instead got {type(parmValue)} type in function - {functionName}")




















class CopyFile:

    # method to check if the file is present else raise error
    @classmethod
    def check_is_file(cls , path):

        p = pathlib.Path(path)

        if(not(p.exists())):
            raise FileExistsError(f"No file found at {path}")
        if(not(p.is_file())):
            raise FileNotFoundError(f"No file found at {path} , recheck file path and permissions")




    # method to check if the dir path is correct else raise error
    @classmethod
    def check_is_dir(cls , path):

        p = pathlib.Path(path)

        if(not(p.exists())):
            raise FileExistsError(f"No dir found at {path}")
        if(not(p.is_dir())):
            raise FileNotFoundError(f"No dir found at {path} , recheck dir path and permissions")
            



    @classmethod
    def copy(cls , source_file , destination_path , chunkSize = 8 , overwrite = True):

        # checking parameters
        checkParameters(CopyFile.copy , paramsDict={
            "source_file" : (source_file , [str]) ,
            "destination_path" : (destination_path , [str]) ,
            "chunkSize" : (chunkSize , [int]) ,
            # "speed_unit" : (speed_unit , [int]) ,
            # "yield_unit" : (yield_unit , [int]) ,
            "overwrite" : (overwrite , [bool]) ,
        })

        # check if the source file and destination path are correct
        cls.check_is_file(source_file)
        cls.check_is_dir(destination_path)

        data_transfer_speedList = deque()


        # getting file name and file size
        filename = pathlib.Path(source_file).name
        fileSize = pathlib.Path(source_file).stat().st_size
        destination_file = pathlib.PurePath(destination_path , filename)


        # converting chunk size into bytes 
        chunkSize = chunkSize * 1000 * 1000

        total_chunks = (fileSize // chunkSize) + 1
        current_chunk = 1

        data_transfer_speedList_sizeLimit = (total_chunks // 100) + 1

        # opening both source file and destination file 
        with open(source_file , "rb") as fil , open(destination_file , "wb") as fil2:
            
            while True:
                startTime = time.perf_counter()

                # reading data
                data = fil.read(chunkSize)

                if not data:
                    break

                # writing data
                fil2.write(data)

                endTime = time.perf_counter()

                # time taken to write data
                timeTaken = endTime - startTime

                # data read + write speed
                # chunk size in MB / time taken to write a chunk
                # units in seconds
                data_transfer_speed = round((chunkSize / 1000 / 1000) / timeTaken , 2)

                if(len(data_transfer_speedList) > data_transfer_speedList_sizeLimit):
                    data_transfer_speedList.popleft()

                data_transfer_speedList.append(data_transfer_speed)

                avgSpeed = round(sum(data_transfer_speedList) / len(data_transfer_speedList) , 2)

                # time left to copy the entire file
                # chunk size left in MB / data transfer speed
                timeLeft = ((total_chunks - current_chunk) * (chunkSize / (1000 * 1000))) / avgSpeed
                timeLeft = round(timeLeft , 2)

                # yields , percentage done , data transfer rate in MB , average overall data transfer speed , time left to copy whole file in seconds
                yield round((current_chunk / total_chunks) * 100 , 2) , data_transfer_speed , avgSpeed , timeLeft
                current_chunk = current_chunk + 1











def main():

    mainPath = "/media/harshnative/HARSH/0Share/testVideo/"
    fileName = "test.mp4"

    genObj = CopyFile.copy(f"{mainPath}{fileName}" , f"{mainPath}Temp" , 8)
    
    
    startTime = time.perf_counter()

    for i in genObj:
        print(i)

    endTime = time.perf_counter()
    timeTaken = endTime - startTime
    print(timeTaken)



if __name__ == "__main__":
    main()