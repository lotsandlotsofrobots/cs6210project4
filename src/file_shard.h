//#pragma once
#ifndef FILE_SHARD_H
#define FILE_SHARD_H

#include <vector>
#include "mapreduce_spec.h"

#include <unistd.h>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <iostream>

struct ShardPart {

};

/* Kinda done - needs to be vector of shardpieces - CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
	int shardID;
	std::string fileName;
	int   offset;     // this will be an offset into an mmapped file
	int   shardSize;  // how many bytes to read
};


/* kinda done - needs to push shard pieces - CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards)
{
		//THIS NEEDS A LITTLE WORK - NEED TO COMBINE SMALL SHARDS INTO BIG ONES

		int shardID = 0;

		for (int fileID = 0; fileID < mr_spec.inputFiles.size(); fileID++)
		{
				std::cout << mr_spec.inputFiles[fileID] << "\n";

				std::string filePath = mr_spec.inputFiles[fileID];
				unsigned long long desiredShardSize = mr_spec.desiredShardSize;

				int fd = open(filePath.c_str(), O_RDWR);

				std::cout << "opened " << filePath << ", fd: " << std::to_string(fd) << "\n";
				std::cout << "Desired shard size: " << std::to_string(desiredShardSize) << "\n";

				std::string dummy;
				std::cin.ignore();
				getline(std::cin, dummy);

				unsigned long long fileSize = lseek(fd, 0, SEEK_END);

				void * fileMMAP = mmap(NULL, fileSize, PROT_READ, MAP_SHARED, fd, 0);

				unsigned long long shardStart = 0;
				unsigned long long candidateShardEnd = 0;
				unsigned long long offset = 0;
				unsigned long long leftOverFromPrevious = 0;

				while (true)
				{
						if( ((char*)fileMMAP)[offset] == '\n' )
						{
								candidateShardEnd = offset;// - 1;
						}

						unsigned long long currentSize = offset - shardStart + 1;

						// if we haven't reached EOF
						if ( offset < fileSize )
						{

								// AND we haven't reached shardSize
								if ( (currentSize < desiredShardSize) )
								{
										offset++;
										continue;
								}
						}

						// if we got here, either offset >= fileSize OR (currentSize - start >= shardSize)
						// so we MAY have a shard

						// first part, if we're at EOF declare that the candidate end and shard it
						if (offset == fileSize)
						{
								candidateShardEnd = offset;

								FileShard fs;
								fs.shardID = shardID;
								shardID++;
								fs.fileName = filePath;
								fs.offset = shardStart;
								fs.shardSize  = candidateShardEnd - shardStart;
								fileShards.push_back(fs);

								// break out so that we move to the next file
								break;
						}
						// if we're not at EOF, then we're big enough to be a shard, but have to check if we have a candidate end.
						else
						{
								// first thing, if we don't have a candidate end, just keep going.
								if (candidateShardEnd == shardStart)
								{
										offset++;
										continue;
								}

								FileShard fs;
								fs.shardID = shardID;
								shardID++;
								fs.fileName = filePath;
								fs.offset = shardStart;
								fs.shardSize = candidateShardEnd - shardStart;
								fileShards.push_back(fs);

								// increment everything so we start this process again
								//shardStart = candidateShardEnd + 2;
								shardStart = candidateShardEnd + 1;
								candidateShardEnd = shardStart;
								offset = shardStart;

								// if we know start -> EOF is < shardSize, just shard it and be done and break out
								if (shardStart + desiredShardSize > fileSize)
								{
										FileShard fs;
										fs.shardID = shardID;
										shardID++;
										fs.fileName = filePath;
										fs.offset = shardStart;
										fs.shardSize = fileSize - shardStart;
										fileShards.push_back(fs);

										break;
								}
						}
				}

				munmap(fileMMAP, fileSize);
		}

		std::cout << "Created " << fileShards.size() << " shards!\n";

		return true;
}

#endif
