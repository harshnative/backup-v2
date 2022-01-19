import pathlib
import time
from tracemalloc import start
from collections import deque
import os


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

        if(not(p.is_file())):
            raise FileNotFoundError(f"No file found at {path} , recheck file path and permissions")




    # method to check if the dir path is correct else raise error
    @classmethod
    def check_is_dir(cls , path):

        p = pathlib.Path(path)

        if(not(p.is_dir())):
            raise FileNotFoundError(f"No dir found at {path} , recheck dir path and permissions")
            



    @classmethod
    def copy(cls , source_file , destination_path , chunkSize = 8 , overwrite = True , makeDirs = False):

        MB = 1000 * 1000

        # checking parameters
        checkParameters(CopyFile.copy , paramsDict={
            "source_file" : (source_file , [str , pathlib.PosixPath , pathlib.PurePath , pathlib.PurePosixPath , pathlib.PureWindowsPath]) ,
            "destination_path" : (destination_path , [str , pathlib.PosixPath , pathlib.PurePath , pathlib.PurePosixPath , pathlib.PureWindowsPath]) ,
            "chunkSize" : (chunkSize , [int]) ,
            # "speed_unit" : (speed_unit , [int]) ,
            # "yield_unit" : (yield_unit , [int]) ,
            "overwrite" : (overwrite , [bool]) ,
            "makeDirs" : (makeDirs , [bool]) ,
        })

        # check if the source file and destination path are correct
        cls.check_is_file(source_file)


        # make dir if not present
        if(makeDirs == False):
            cls.check_is_dir(destination_path)
        else:
            try:
                cls.check_is_dir(destination_path)
            except FileNotFoundError:
                os.makedirs(destination_path)


        data_transfer_speedList = deque()


        # getting file name and file size
        filename = pathlib.Path(source_file).name
        fileSize = pathlib.Path(source_file).stat().st_size
        destination_file = pathlib.PurePath(destination_path , filename)

        # check if file already exist and overwrite is false
        if(not(overwrite) and pathlib.Path(destination_file).exists()):
            raise FileExistsError("file already exist , you want to replace run with overwrite = True")

        # converting chunk size into bytes 
        chunkSize = chunkSize * MB

        total_chunks = (fileSize // chunkSize) + 1
        current_chunk = 1

        data_transfer_speedList_sizeLimit = (total_chunks // 100) + 1

        bytesWritten = 0

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
                data_transfer_speed = round((chunkSize / MB) / timeTaken , 2)

                if(len(data_transfer_speedList) > data_transfer_speedList_sizeLimit):
                    data_transfer_speedList.popleft()

                data_transfer_speedList.append(data_transfer_speed)

                avgSpeed = round(sum(data_transfer_speedList) / len(data_transfer_speedList) , 2)
                
                lenData = len(data)
                bytesWritten = bytesWritten + lenData

                # time left to copy the entire file
                # chunk size left in MB / data transfer speed
                timeLeft = ((fileSize - bytesWritten) / MB) / avgSpeed
                timeLeft = round(timeLeft , 2)


                yieldDict = {
                    # percentage done
                    "percentage_done" : round((current_chunk / total_chunks) * 100 , 2) , 
                    
                    # data transfer rate in MB
                    "data_transfer_speed" : data_transfer_speed , 
                    
                    # average overall data transfer speed
                    "avg_data_transfer_speed" : avgSpeed , 
                    
                    # time left to copy whole file in seconds
                    "timeLeft" : timeLeft , 
                    
                    # data written in bytes
                    "bytesWritten" : bytesWritten , 
                    
                    # total data/file size in bytes
                    "fileSize" : fileSize,

                    # data written in this round
                    "lenData" : lenData
                }

                yield yieldDict
                current_chunk = current_chunk + 1















    @classmethod
    def copy_multiple_files(cls , filesDict , chunkSize = 8 , overwrite = True , makeDirs = False):
        
        MB = 1000 * 1000

        function_start_time = time.perf_counter()

        # checking parameters
        checkParameters(CopyFile.copy , paramsDict={
            "filesDict" : (filesDict , [dict]) ,
            "chunkSize" : (chunkSize , [int]) ,
            "overwrite" : (overwrite , [bool]) ,
            "makeDirs" : (makeDirs , [bool]) ,
        })

        totalSize = filesDict.get("totalSize" , None)

        if(totalSize == None):
            raise ValueError("filesDict does not have any key = totalSize , make sure you get filesDict from get_file_dict() method")

        count = filesDict.get("count" , 0)

        total_bytesWritten = 0

        for key , subFileDict in filesDict.items():

            if(not(type(subFileDict) == dict)):
                continue

            sourceFile = subFileDict["sourceFile"]
            # relativePath = subFileDict["relativePath"]
            destinationPath = subFileDict["destinationPath"]
            # fileStats = subFileDict["fileStats"]
            # fileSize = fileStats.st_size


            for copyYield in cls.copy(sourceFile , destinationPath , chunkSize , overwrite , makeDirs):
                
                current_file_percentage = copyYield["percentage_done"]
                data_transfer_speed = copyYield["data_transfer_speed"]
                avg_data_transfer_speed = copyYield["avg_data_transfer_speed"]
                current_file_timeLeft = copyYield["timeLeft"]
                current_file_bytesWritten = copyYield["bytesWritten"]
                current_file_fileSize = copyYield["fileSize"]
                current_file_lenData = copyYield["lenData"]

                total_bytesWritten = total_bytesWritten + current_file_lenData
                
                # total amount of data left to copy = total data size - total data written till now
                total_data_left = totalSize - total_bytesWritten 


                # calculating avg speed from time passed and total data written till now
                current_time = time.perf_counter() 

                time_elapsed = current_time - function_start_time

                overall_avg_speed = round((total_bytesWritten / MB) / time_elapsed , 2)

                # total time left
                total_time_left = round((total_data_left / MB) / avg_data_transfer_speed , 2) 
                
                # total_time_left accurate version
                total_time_left_acc = round((total_data_left / MB) / overall_avg_speed , 2) 
        
                # total percentage = total_bytesWritten / totalSize * 100
                total_percentage =  round((total_bytesWritten / totalSize) * 100 , 3)

                resultDict = {
                    "current_file" : key , 
                    "total_time_left" : total_time_left , 
                    "total_time_left_acc" : total_time_left_acc , 
                    "total_bytesWritten" : total_bytesWritten , 
                    "totalSize" : totalSize , 
                    "total_percentage" : total_percentage , 
                    "data_transfer_speed" : data_transfer_speed , 
                    "avg_data_transfer_speed" : avg_data_transfer_speed , 
                    "overall_avg_speed" : overall_avg_speed , 
                    "current_file_timeLeft" : current_file_timeLeft , 
                    "current_file_bytesWritten" : current_file_bytesWritten , 
                    "current_file_size" : current_file_fileSize , 
                    "current_file_percentage" : current_file_percentage , 
                    "current_file_lenData" : current_file_lenData , 
                }

                yield resultDict



        






















    @classmethod
    def get_file_dict(cls , source_folder_path , destination_folder_path , createFolders = False):

        checkParameters(CopyFile.copy , paramsDict={
        "folder_path" : (source_folder_path , [str]) , 
        })

        filesDict = {}
        totalSize = 0
        count = 0

        # traverse the dir recursive order
        for elem in pathlib.Path(source_folder_path).rglob("*"):

            # get paths and file stats
            if(elem.is_file()):
                absolutePath = elem.absolute()
                relativePath = os.path.relpath(absolutePath, source_folder_path)
                destinationPath = pathlib.Path(destination_folder_path , relativePath).parent
                fileSize = elem.stat().st_size
                # fileStats = elem.stat()

                totalSize = totalSize + fileSize
                count = count + 1

                # add data to result dict
                filesDict[count] = {
                    "sourceFile" : absolutePath , 
                    # "relativePath" : relativePath , 
                    "destinationPath" : destinationPath , 
                    # "fileStats" : fileStats , 
                }

                yield count


            # make the dirs also in destination path , no matter even it is empty
            # to avoid copying empty dir , run with createFolders = False
            elif(elem.is_dir() and createFolders):
                absolutePath = elem.absolute()
                relativePath = os.path.relpath(absolutePath, source_folder_path)
                destinationPath = pathlib.Path(destination_folder_path , relativePath)

                if(not(destinationPath.exists())):
                    os.makedirs(destinationPath)

                count = count + 1
                yield count

        
        filesDict["totalSize"] = totalSize
        filesDict["count"] = count

        return filesDict



    











def __fileCopyTest():

    mainPath = "/media/harshnative/HARSH/0Share/tempdump/temp/"
    mainPath2 = "/media/harshnative/HARSH/0Share/tempdump/temp2/"
    fileName = "test1.mp4"

    genObj = CopyFile.copy(f"{mainPath}{fileName}" , f"{mainPath2}" , 8 , makeDirs=True)
    
    
    startTime = time.perf_counter()

    for i in genObj:
        print(i)

    endTime = time.perf_counter()
    timeTaken = endTime - startTime
    print(timeTaken)





def __fileCopy_folderTest():

    # genObj = CopyFile.get_file_dict("/media/harshnative/HARSH/0Share/tempdump/temp/" , "/media/harshnative/HARSH/0Share/tempdump/temp2/")
    genObj = CopyFile.get_file_dict("/media/harshnative/HARSH/0Share/tempdump/easySedGui/" , "/media/harshnative/HARSH/0Share/tempdump/easySedGui_temp/" , createFolders=True)
    # CopyFile.get_file_dict("/home/harshnative/")

    print()
    while(True):
        try:
            filesFound = next(genObj)

            print(f"\rfiles found so far = {filesFound}" , end="")
        except StopIteration as ex:
            resultDict = ex.value
            break
    print()

    input()

    for _,i in resultDict.items():
        print(i)

    input()

    for i in CopyFile.copy_multiple_files(resultDict , makeDirs=True):
        print(i)





def main():
    pass




if __name__ == "__main__":
    # main()
    # __fileCopyTest()
    __fileCopy_folderTest()